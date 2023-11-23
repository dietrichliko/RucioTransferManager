"""Rucio Transfer Manager - sqlalchemy model.
"""
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy_state_machine import StateConfig, StateMixin

NEW = "new"
SUBSCRIBED = "subscribed"
TRANSFERED = "transferred"
DONE="done"
ERROR="error"

class Base(DeclarativeBase):
    pass


class Dataset(Base,StateMixin):
    __tablename__ = "datasets"

    state_config = StateConfig(
        initial=NEW,
        states=[NEW, SUBSCRIBED, TRANSFERED, DONE],
        transitions=[
            ["subscribe", NEW, SUBSCRIBED],
            ["transfered", SUBSCRIBED, TRANSFERED],
            ["verified", TRANSFERED, DONE],
            ["error", "*", ERROR],
        ],
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(sa.String, unique=True)
    site: Mapped[str] = mapped_column(sa.String)
    subscription: Mapped[str] = mapped_column(sa.String, nullable=True)
    files: Mapped[list["File"]] = relationship()
    status: Mapped[str] = mapped_column(sa.String, index=True)
    ok_cnt: Mapped[int] = mapped_column(sa.Integer)
    replicating_cnt: Mapped[int] = mapped_column(sa.Integer)
    stuck_cnt: Mapped[int] = mapped_column(sa.Integer)

    def __repr__(self) -> str:
        return f"Dataset({self.name})"

sa.event.listen(Dataset, "init", Dataset.init_state_machine)
sa.event.listen(Dataset, "load", Dataset.init_state_machine)



class File(Base):
    __tablename__ = "files"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    dataset_id: Mapped[int] = mapped_column(sa.ForeignKey("datasets.id"))
    lfn: Mapped[str] = mapped_column(sa.String, unique=True)
    size: Mapped[int] = mapped_column(sa.Integer)
    checksum: Mapped[int] = mapped_column(sa.Integer)
    events: Mapped[int] = mapped_column(sa.Integer)

    def __repr__(self) -> str:
        return f"File({self.lfn})"
