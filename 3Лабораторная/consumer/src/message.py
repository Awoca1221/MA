from dataclasses import dataclass, asdict
import json

MESSAGE_TYPE_NOTIFICATION = "Notification"

@dataclass
class Notification:
    text: str
    type: str = MESSAGE_TYPE_NOTIFICATION

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, raw: bytes | str) -> "Notification":
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = json.loads(raw)
        return cls(
            text=data["text"],
            type=data.get("type", MESSAGE_TYPE_NOTIFICATION),
        )
