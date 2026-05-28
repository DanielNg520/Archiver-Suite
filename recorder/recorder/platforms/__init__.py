"""recorder.platforms — live-platform detection strategies."""

from .base import LivePlatform
from .tiktok import TikTokLivePlatform

__all__ = ["LivePlatform", "TikTokLivePlatform"]
