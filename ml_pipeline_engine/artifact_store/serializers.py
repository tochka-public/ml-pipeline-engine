import io
import json
import pickle
import typing as t
from abc import ABC, abstractmethod

from ml_pipeline_engine.artifact_store.enums import DataFormat

SerializableObjectT = t.Any


class SerializerInitializationError(Exception):
    pass


class Serializer(ABC):
    @abstractmethod
    def dump(self, obj: SerializableObjectT, fp: t.IO) -> None:
        ...

    @abstractmethod
    def load(self, fp: t.IO) -> SerializableObjectT:
        ...

    @abstractmethod
    def get_default_io(self) -> t.IO:
        ...


class PickleSerializer(Serializer):
    def dump(self, obj: SerializableObjectT, fp: t.IO) -> None:
        pickle.dump(obj, fp)
        fp.seek(0)

    def load(self, fp: t.IO) -> SerializableObjectT:
        fp.seek(0)
        return pickle.load(fp)

    def get_default_io(self) -> t.IO:
        return io.BytesIO()


class JSONSerializer(Serializer):
    def dump(self, obj: SerializableObjectT, fp: t.IO) -> None:
        json.dump(obj, fp, indent=4, ensure_ascii=False)
        fp.seek(0)

    def load(self, fp: t.IO) -> SerializableObjectT:
        fp.seek(0)
        return json.load(fp)

    def get_default_io(self) -> t.IO:
        return io.StringIO()


class SerializerFactory:
    @staticmethod
    def from_data_format(fmt: DataFormat) -> Serializer:
        if fmt == DataFormat.PICKLE:
            return PickleSerializer()

        return JSONSerializer()

    def from_extension(self, extension: str) -> Serializer:
        try:
            fmt = DataFormat(extension)
        except ValueError:
            raise SerializerInitializationError(f'No suitable serializer for {extension} extension')

        return self.from_data_format(fmt)


serializer_factory = SerializerFactory()
