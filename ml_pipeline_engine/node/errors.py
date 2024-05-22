class BaseNodeError(Exception):
    pass


class ClassExpectedError(BaseNodeError):
    pass


class RunMethodExpectedError(BaseNodeError):
    pass


class DefaultMethodDoesNotExistError(BaseNodeError):
    pass
