from distutils.dir_util import copy_tree

import importlib_resources


def copy_resources(source_package: str, *resource_path: str, target_dir: str) -> None:
    """
    Copy resources from a Python package to a target directory.

    Args:
        source_package: The name of the Python package containing the resources.
        resource_path: The path to the resources within the package.
        target_dir: The directory where the resources will be copied.
    """

    with importlib_resources.path(source_package, *resource_path) as resource_dir:
        copy_tree(resource_dir, target_dir)
