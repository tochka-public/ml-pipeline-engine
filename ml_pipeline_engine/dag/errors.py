class BaseDagError(Exception):
    pass


class OneOfDoesNotHaveResultError(BaseDagError):
    pass


class OneOfSubgraphDagError(BaseDagError):
    pass


class RecurrentSubgraphDoesNotHaveResultError(BaseDagError):
    pass
