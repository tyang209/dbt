from typing import (
    Dict, List, MutableMapping
)

from dbt.contracts.graph.parsed import ParsedMacro
from dbt.exceptions import raise_duplicate_macro_name


MacroNamespace = Dict[str, ParsedMacro]


# This class builds the MacroResolver by adding macros
# to various categories for finding macros in the right order,
# so that higher precedence macros are found first.
class MacroResolver:
    def __init__(
        self,
        root_package: str,
        internal_packages: List[str],
        macros: MutableMapping[str, ParsedMacro],
    ) -> None:
        self.root_package = root_package
        self.macros = macros
        # internal packages comes from get_adapter_package_names
        self.internal_package_names = set(internal_packages)
        self.internal_package_names_order = internal_packages
        # To be filled in from macros.
        self.internal_packages: Dict[str, MacroNamespace] = {}
        # Non-internal packages
        self.packages: Dict[str, MacroNamespace] = {}
        self.root_package_macros: MacroNamespace = {}
        self.local_package_macros: MacroNamespace = {}

        # add the macros to internal_packages, packages, and root_packages
        self.add_macros()

        # Iterate in reverse-order and overwrite: the packages that are first
        # in the list are the ones we want to "win".
        self.internal_packages_namespace: MacroNamespace = {}
        for pkg in reversed(self.internal_package_names_order):
            if pkg in self.internal_packages:
                self.internal_packages_namespace.update(
                    self.internal_packages[pkg])

    def _add_macro_to(
        self,
        package_namespaces: Dict[str, MacroNamespace],
        macro: ParsedMacro,
    ):
        if macro.package_name in package_namespaces:
            namespace = package_namespaces[macro.package_name]
        else:
            namespace = {}
            package_namespaces[macro.package_name] = namespace

        if macro.name in namespace:
            raise_duplicate_macro_name(
                macro, macro, macro.package_name
            )
        package_namespaces[macro.package_name][macro.name] = macro

    def add_macro(self, macro: ParsedMacro):
        macro_name: str = macro.name

        # internal macros (from plugins) will be processed separately from
        # project macros, so store them in a different place
        if macro.package_name in self.internal_package_names:
            self._add_macro_to(self.internal_packages, macro)
        else:
            # if it's not an internal package
            self._add_macro_to(self.packages, macro)
            # add to root_package_macros if it's in the root package
            if macro.package_name == self.root_package:
                self.root_package_macros[macro_name] = macro

    def add_macros(self):
        for macro in self.macros.values():
            self.add_macro(macro)

    def get_macro_id(self, local_package, macro_name):
        local_package_macros = {}
        if (local_package not in self.internal_package_names and
                local_package in self.packages):
            local_package_macros = self.packages[local_package]
        # First: search the local packages for this macro
        if macro_name in local_package_macros:
            return local_package_macros[macro_name].unique_id
        # Second: search root package macros
        if macro_name in self.root_package_macros:
            return self.root_package_macros[macro_name].unique_id
        # Third: search miscellaneous non-internal packages
        for fnamespace in self.packages.values():
            if macro_name in fnamespace:
                return fnamespace[macro_name].unique_id
        # Fourth: search all internal packages
        if macro_name in self.internal_packages_namespace:
            return self.internal_packages_namespace[macro_name].unique_id
        return None
