from enum import Enum


class NodeField(str, Enum):
    is_switch = 'is_switch'
    is_first_success_pool = 'is_first_success_pool'
    is_first_success = 'is_first_success'
    start_node = 'start_node'
    max_iterations = 'max_iterations'
    additional_data = 'additional_data'


class EdgeField(str, Enum):
    kwarg_name = 'kwarg_name'
    is_switch = 'is_switch'
    case_branch = 'case_branch'
    is_first_success = 'is_first_success'
