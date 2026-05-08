"""Pricing calculations for Claude models.

This module provides the PricingCalculator class for calculating costs
based on token usage and model pricing. It supports all Claude model types
(Opus, Sonnet, Haiku) and provides both simple and detailed cost calculations
with caching.
"""

from typing import Any, Dict, Optional

from claude_monitor.core.models import CostMode, TokenCounts, normalize_model_name


class PricingCalculator:
    """Calculates costs based on model pricing with caching support.

    This class provides methods for calculating costs for individual models/tokens
    as well as detailed cost breakdowns for collections of usage entries.
    It supports custom pricing configurations and caches calculations for performance.

    Features:
    - Configurable pricing (from config or custom)
    - Fallback hardcoded pricing for robustness
    - Caching for performance
    - Support for all token types including cache
    - Backward compatible with both APIs
    """

    FALLBACK_PRICING: Dict[str, Dict[str, float]] = {
        "opus": {
            "input": 15.0,
            "output": 75.0,
            "cache_creation": 18.75,
            "cache_read": 1.5,
        },
        "sonnet": {
            "input": 3.0,
            "output": 15.0,
            "cache_creation": 3.75,
            "cache_read": 0.3,
        },
        "haiku": {
            "input": 0.25,
            "output": 1.25,
            "cache_creation": 0.3,
            "cache_read": 0.03,
        },
    }

    # Third-party model pricing (per million tokens)
    # GLM models (Z.AI)
    MODEL_PRICING: Dict[str, Dict[str, float]] = {
        # GLM-4 family
        "glm-4": {"input": 0.6, "output": 2.2, "cache_creation": 0.75, "cache_read": 0.06},
        "glm-4-5": {"input": 0.6, "output": 2.2, "cache_creation": 0.75, "cache_read": 0.06},
        "glm-4-5-air": {"input": 0.13, "output": 0.85, "cache_creation": 0.16, "cache_read": 0.013},
        "glm-4-5v": {"input": 0.6, "output": 1.8, "cache_creation": 0.75, "cache_read": 0.06},
        "glm-4-7": {"input": 0.4, "output": 1.75, "cache_creation": 0.5, "cache_read": 0.04},
        "glm-4-7-flash": {"input": 0.06, "output": 0.4, "cache_creation": 0.075, "cache_read": 0.006},
        # GLM-5 family
        "glm-5": {"input": 0.6, "output": 1.92, "cache_creation": 0.75, "cache_read": 0.06},
        "glm-5-turbo": {"input": 1.2, "output": 4.0, "cache_creation": 1.5, "cache_read": 0.12},
        "glm-5-1": {"input": 5.0, "output": 25.0, "cache_creation": 6.25, "cache_read": 0.5},
        # GLM-5V
        "glm-5-turbo-v": {"input": 1.2, "output": 4.0, "cache_creation": 1.5, "cache_read": 0.12},
        # MiniMax models
        "minimax-m2.7": {"input": 0.3, "output": 1.2, "cache_creation": 0.06, "cache_read": 0.375},
        "minimax-m2.7-highspeed": {"input": 0.6, "output": 2.4, "cache_creation": 0.06, "cache_read": 0.375},
        # DeepSeek V4 models
        "deepseek-v4-flash": {"input": 0.14, "output": 0.28, "cache_creation": 0.175, "cache_read": 0.0036},
        "deepseek-v4-pro": {"input": 0.44, "output": 0.88, "cache_creation": 0.55, "cache_read": 0.0036},
        # DeepSeek V3 / R1 models (legacy)
        "deepseek-chat": {"input": 0.14, "output": 0.28, "cache_creation": 0.175, "cache_read": 0.014},
        "deepseek-reasoner": {"input": 0.14, "output": 0.28, "cache_creation": 0.175, "cache_read": 0.014},
    }

    def __init__(
        self, custom_pricing: Optional[Dict[str, Dict[str, float]]] = None
    ) -> None:
        """Initialize with optional custom pricing.

        Args:
            custom_pricing: Optional custom pricing dictionary to override defaults.
                          Should follow same structure as MODEL_PRICING.
        """
        # Build pricing dict with all available models
        all_pricing: Dict[str, Dict[str, float]] = {
            "claude-3-opus": self.FALLBACK_PRICING["opus"],
            "claude-3-sonnet": self.FALLBACK_PRICING["sonnet"],
            "claude-3-haiku": self.FALLBACK_PRICING["haiku"],
            "claude-3-5-sonnet": self.FALLBACK_PRICING["sonnet"],
            "claude-3-5-haiku": self.FALLBACK_PRICING["haiku"],
            "claude-sonnet-4-20250514": self.FALLBACK_PRICING["sonnet"],
            "claude-opus-4-20250514": self.FALLBACK_PRICING["opus"],
        }
        # Merge third-party pricing
        all_pricing.update(self.MODEL_PRICING)
        self.pricing = custom_pricing or all_pricing
        self._cost_cache: Dict[str, float] = {}

    def calculate_cost(
        self,
        model: str,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cache_creation_tokens: int = 0,
        cache_read_tokens: int = 0,
        tokens: Optional[TokenCounts] = None,
        strict: bool = False,
    ) -> float:
        """Calculate cost with flexible API supporting both signatures.

        Args:
            model: Model name
            input_tokens: Number of input tokens (ignored if tokens provided)
            output_tokens: Number of output tokens (ignored if tokens provided)
            cache_creation_tokens: Number of cache creation tokens
            cache_read_tokens: Number of cache read tokens
            tokens: Optional TokenCounts object (takes precedence)

        Returns:
            Total cost in USD
        """
        # Handle synthetic model
        if model == "<synthetic>":
            return 0.0

        # Support TokenCounts object
        if tokens is not None:
            input_tokens = tokens.input_tokens
            output_tokens = tokens.output_tokens
            cache_creation_tokens = tokens.cache_creation_tokens
            cache_read_tokens = tokens.cache_read_tokens

        # Create cache key
        cache_key = (
            f"{model}:{input_tokens}:{output_tokens}:"
            f"{cache_creation_tokens}:{cache_read_tokens}"
        )

        # Check cache
        if cache_key in self._cost_cache:
            return self._cost_cache[cache_key]

        # Get pricing for model
        pricing = self._get_pricing_for_model(model, strict=strict)

        # Calculate costs (pricing is per million tokens)
        cost = (
            (input_tokens / 1_000_000) * pricing["input"]
            + (output_tokens / 1_000_000) * pricing["output"]
            + (cache_creation_tokens / 1_000_000)
            * pricing.get("cache_creation", pricing["input"] * 1.25)
            + (cache_read_tokens / 1_000_000)
            * pricing.get("cache_read", pricing["input"] * 0.1)
        )

        # Round to 6 decimal places
        cost = round(cost, 6)

        # Cache result
        self._cost_cache[cache_key] = cost
        return cost

    def _get_pricing_for_model(
        self, model: str, strict: bool = False
    ) -> Dict[str, float]:
        """Get pricing for a model with optional fallback logic.

        Args:
            model: Model name
            strict: If True, raise KeyError for unknown models

        Returns:
            Pricing dictionary with input/output/cache costs

        Raises:
            KeyError: If strict=True and model is unknown
        """
        # Try normalized model name first
        normalized = normalize_model_name(model)

        # Check configured pricing
        if normalized in self.pricing:
            pricing = self.pricing[normalized]
            # Ensure cache pricing exists
            if "cache_creation" not in pricing:
                pricing["cache_creation"] = pricing["input"] * 1.25
            if "cache_read" not in pricing:
                pricing["cache_read"] = pricing["input"] * 0.1
            return pricing

        # Check original model name
        if model in self.pricing:
            pricing = self.pricing[model]
            if "cache_creation" not in pricing:
                pricing["cache_creation"] = pricing["input"] * 1.25
            if "cache_read" not in pricing:
                pricing["cache_read"] = pricing["input"] * 0.1
            return pricing

        # If strict mode, raise KeyError for unknown models
        if strict:
            raise KeyError(f"Unknown model: {model}")

        # Fallback to hardcoded pricing based on model type
        model_lower = model.lower()
        if "opus" in model_lower:
            return self.FALLBACK_PRICING["opus"]
        if "haiku" in model_lower:
            return self.FALLBACK_PRICING["haiku"]
        # Default to Sonnet pricing
        return self.FALLBACK_PRICING["sonnet"]

    def calculate_cost_for_entry(
        self, entry_data: Dict[str, Any], mode: CostMode
    ) -> float:
        """Calculate cost for a single entry (backward compatibility).

        Args:
            entry_data: Entry data dictionary
            mode: Cost mode (for backward compatibility)

        Returns:
            Cost in USD
        """
        # If cost is present and mode is cached, use it
        if mode.value == "cached":
            cost_value = entry_data.get("costUSD") or entry_data.get("cost_usd")
            if cost_value is not None:
                return float(cost_value)

        # Otherwise calculate from tokens
        model = entry_data.get("model") or entry_data.get("Model")
        if not model:
            raise KeyError("Missing 'model' key in entry_data")

        # Extract token counts with different possible keys
        input_tokens = entry_data.get("inputTokens", 0) or entry_data.get(
            "input_tokens", 0
        )
        output_tokens = entry_data.get("outputTokens", 0) or entry_data.get(
            "output_tokens", 0
        )
        cache_creation = entry_data.get(
            "cacheCreationInputTokens", 0
        ) or entry_data.get("cache_creation_tokens", 0)
        cache_read = (
            entry_data.get("cacheReadInputTokens", 0)
            or entry_data.get("cache_read_input_tokens", 0)
            or entry_data.get("cache_read_tokens", 0)
        )

        return self.calculate_cost(
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_creation_tokens=cache_creation,
            cache_read_tokens=cache_read,
        )
