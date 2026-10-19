import json
from datetime import datetime, timezone
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class IngestedRecord(Base):
    """Normalized ingested records table."""

    __tablename__ = "ingested_records"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    entity_id = Column(String(128), nullable=False, index=True)
    source_id = Column(String(64), nullable=False, index=True)
    event_timestamp = Column(DateTime, nullable=False, index=True)
    category = Column(String(64), nullable=False, default="general", index=True)
    payload_hash = Column(String(64), nullable=False, index=True)
    numeric_value = Column(Float, nullable=False, default=0.0)
    raw_payload = Column(Text, nullable=False)
    metadata_json = Column(Text, nullable=True)

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    __table_args__ = (
        Index("idx_ingested_source_time", "source_id", "event_timestamp"),
        Index("idx_ingested_entity_hash", "entity_id", "payload_hash"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "entity_id": self.entity_id,
            "source_id": self.source_id,
            "event_timestamp": self.event_timestamp.isoformat() if self.event_timestamp else None,
            "category": self.category,
            "payload_hash": self.payload_hash,
            "numeric_value": self.numeric_value,
            "raw_payload": json.loads(self.raw_payload) if self.raw_payload else {},
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class DeadLetterPayload(Base):
    """Quarantine store for unparseable or schema-violating external payloads."""

    __tablename__ = "dead_letter_payloads"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    source_id = Column(String(64), nullable=False, index=True)
    error_reason = Column(Text, nullable=False)
    raw_data = Column(Text, nullable=False)
    quarantined_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    def to_dict(self):
        return {
            "id": self.id,
            "source_id": self.source_id,
            "error_reason": self.error_reason,
            "raw_data": self.raw_data,
            "quarantined_at": self.quarantined_at.isoformat() if self.quarantined_at else None,
        }
