class ArtifactStoreError(Exception):
    pass


class ArtifactDoesNotExist(ArtifactStoreError):
    pass


class ArtifactAlreadyExists(ArtifactStoreError):
    pass
