import pytest

from vortex.engine import Engine


# Create a dummy subclass for testing purposes
class DummyEngine(Engine):
    def write_stream(self, dataframe, stage, batch_id, app_id, spark):
        pass

    def read_stream(self, spark):
        pass

    def write_batch(self, dataframe, spark):
        pass

    def read_batch(self, spark):
        pass


def test_dummy_engine_instantiation():
    engine = DummyEngine()
    assert isinstance(engine, Engine)


def test_dummy_engine_methods():
    engine = DummyEngine()

    assert engine.write_stream(None, None, None, None, None) is None
    assert engine.read_stream(None) is None
    assert engine.write_batch(None, None) is None
    assert engine.read_batch(None) is None
