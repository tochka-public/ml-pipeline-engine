class BaseBuilderError(Exception):
    pass


class BaseAnnotationError(BaseBuilderError):
    pass


class NonRedefinedGenericTypeError(BaseAnnotationError):
    pass


class UndefinedAnnotation(BaseAnnotationError):
    pass


class UndefinedParamAnnotation(BaseAnnotationError):
    pass


class IncorrectBaseClass(BaseBuilderError):
    pass


class IncorrectTypeClass(BaseBuilderError):
    pass


class IncorrectRecurrentMixinClass(BaseBuilderError):
    pass


class IncorrectParamsRecurrentNode(BaseBuilderError):
    pass
