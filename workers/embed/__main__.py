import json

from py4j.java_gateway import JavaGateway, GatewayParameters, CallbackServerParameters
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")  # 384 dims


class EmbedIn(BaseModel):
    text: str


class EmbedOut(BaseModel):
    vector: list[float]


class Embedding(object):
    class Java:
        implements = ['udem.taln.wrapper.vectors.VecInterface']

    @staticmethod
    def getVector(inp: EmbedIn) -> str:
        vec = model.encode([inp.text], convert_to_numpy=True)[0].tolist()
        return json.dumps({"vector": vec}, ensure_ascii=False, default=str)


def main():
    gateway = JavaGateway(
        gateway_parameters=GatewayParameters(address="127.0.0.1", port=25333),
        callback_server_parameters=CallbackServerParameters()
    )
    gateway.entry_point.registerPythonObject(Embedding())
    print("Python side registered. Waiting for calls...")
    import time
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
