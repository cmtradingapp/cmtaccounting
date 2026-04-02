"""Transformer registry with auto-discovery.

Manages PSP and Bank transformers. Supports:
- Manual registration
- Auto-discovery from transformers/psp/ and transformers/bank/ packages
- Filename-based lookup to find the right transformer for a file
"""

import importlib
import pkgutil
from typing import Optional

from transformers.base import PSPTransformer, BankTransformer


class TransformerRegistry:
    """Central registry for all PSP and Bank transformers."""

    def __init__(self):
        self._psp: dict[str, PSPTransformer] = {}
        self._bank: dict[str, BankTransformer] = {}

    def register_psp(self, transformer: PSPTransformer):
        """Register a PSP transformer by its canonical name."""
        self._psp[transformer.psp_name] = transformer

    def register_bank(self, transformer: BankTransformer):
        """Register a bank transformer by its canonical name."""
        self._bank[transformer.bank_name] = transformer

    def get_psp(self, name: str) -> Optional[PSPTransformer]:
        """Get a PSP transformer by canonical name."""
        return self._psp.get(name)

    def get_bank(self, name: str) -> Optional[BankTransformer]:
        """Get a bank transformer by canonical name."""
        return self._bank.get(name)

    def get_transformer_for_file(self, filename: str) -> Optional[PSPTransformer | BankTransformer]:
        """Find the first transformer whose file_patterns match the filename."""
        for t in self._psp.values():
            if t.matches_file(filename):
                return t
        for t in self._bank.values():
            if t.matches_file(filename):
                return t
        return None

    def get_psp_for_file(self, filename: str) -> Optional[PSPTransformer]:
        """Find a PSP transformer matching the filename."""
        for t in self._psp.values():
            if t.matches_file(filename):
                return t
        return None

    def get_bank_for_file(self, filename: str) -> Optional[BankTransformer]:
        """Find a bank transformer matching the filename."""
        for t in self._bank.values():
            if t.matches_file(filename):
                return t
        return None

    @property
    def psp_names(self) -> list[str]:
        return list(self._psp.keys())

    @property
    def bank_names(self) -> list[str]:
        return list(self._bank.keys())

    def auto_discover(self):
        """Scan transformers/psp/ and transformers/bank/ for transformer classes.

        Any module in those packages that defines a subclass of PSPTransformer
        or BankTransformer will have that class instantiated and registered.
        """
        for package_name, base_cls, register_fn in [
            ("transformers.psp", PSPTransformer, self.register_psp),
            ("transformers.bank", BankTransformer, self.register_bank),
        ]:
            try:
                package = importlib.import_module(package_name)
            except ImportError:
                continue

            for importer, modname, ispkg in pkgutil.iter_modules(package.__path__):
                module = importlib.import_module(f"{package_name}.{modname}")
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (isinstance(attr, type)
                            and issubclass(attr, base_cls)
                            and attr is not base_cls
                            and not getattr(attr, '__abstractmethods__', None)):
                        try:
                            instance = attr()
                            register_fn(instance)
                        except Exception:
                            pass


# Global registry instance — auto-discovers on import
registry = TransformerRegistry()
