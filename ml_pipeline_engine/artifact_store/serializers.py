import io
import json
import pickle
import typing as t
from abc import ABC
from abc import abstractmethod

from anyio import AsyncFile

from ml_pipeline_engine.artifact_store.enums import DataFormat

SerializableObjectT = t.Any


class SerializerInitializationError(Exception):
    pass


class Serializer(ABC):
    @abstractmethod
    async def dump(self, obj: SerializableObjectT, fp: t.Union[t.IO, AsyncFile]) -> None: ...

    @abstractmethod
    async def load(self, fp: t.Union[t.IO, AsyncFile]) -> SerializableObjectT: ...

    @abstractmethod
    def get_default_io(self) -> t.IO: ...


class PickleSerializer(Serializer):
    async def dump(self, obj: SerializableObjectT, fp: t.Union[t.IO, AsyncFile]) -> None:
        if isinstance(fp, AsyncFile):
            await fp.write(pickle.dumps(obj))
            return
        pickle.dump(obj, fp)
        fp.seek(0)

    async def load(self, fp: t.Union[t.IO, AsyncFile]) -> SerializableObjectT:
        if isinstance(fp, AsyncFile):
            content = await fp.read()
            return pickle.loads(content)
        fp.seek(0)
        return pickle.load(fp)

    def get_default_io(self) -> t.IO:
        return io.BytesIO()


class JSONSerializer(Serializer):
    async def dump(self, obj: SerializableObjectT, fp: t.Union[t.IO, AsyncFile]) -> None:
        if isinstance(fp, AsyncFile):
            await fp.write(json.dumps(obj, indent=4, ensure_ascii=False))
            return
        json.dump(obj, fp, indent=4, ensure_ascii=False)
        fp.seek(0)

    async def load(self, fp: t.Union[t.IO, AsyncFile]) -> SerializableObjectT:
        if isinstance(fp, AsyncFile):
            content = await fp.read()
            return json.loads(content)
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
            raise SerializerInitializationError(f'No suitable serializer for {extension} extension') from None

        return self.from_data_format(fmt)


serializer_factory = SerializerFactory()
