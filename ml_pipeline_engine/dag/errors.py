class BaseDagError(Exception):
    pass


class OneOfDoesNotHaveResultError(BaseDagError):
    pass


class RecurrentSubgraphDoesNotHaveResultError(BaseDagError):
    pass
