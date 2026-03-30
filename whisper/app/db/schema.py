from datetime import date as dt_date, datetime as dt_datetime, timedelta as dt_timedelta

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Interval,
    LargeBinary,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class SeriesDataRecord(Base):
    __tablename__ = "series_data"

    series_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    series_name: Mapped[str | None] = mapped_column(String(255), nullable=True)


class SeriesLecturerRecord(Base):
    __tablename__ = "series_lecturers"

    series_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("series_data.series_id", ondelete="CASCADE"),
        primary_key=True,
    )
    lecturer_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("lecturer_data.lecturer_id", ondelete="CASCADE"),
        primary_key=True,
    )


class LecturerDataRecord(Base):
    __tablename__ = "lecturer_data"

    lecturer_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lecturer_name: Mapped[str] = mapped_column(String(255), nullable=False)


class LectureDataRecord(Base):
    __tablename__ = "lecture_data"

    lecture_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    language: Mapped[str | None] = mapped_column(String(50), nullable=True)
    date: Mapped[dt_date | None] = mapped_column(Date, nullable=True)
    series_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("series_data.series_id", ondelete="SET NULL"),
        nullable=True,
    )
    semester: Mapped[str | None] = mapped_column(String(50), nullable=True)
    duration: Mapped[dt_timedelta | None] = mapped_column(Interval, nullable=True)
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    video_mp4: Mapped[str | None] = mapped_column(String(255), nullable=True)
    desktop_mp4: Mapped[str | None] = mapped_column(String(255), nullable=True)
    podcast_mp4: Mapped[str | None] = mapped_column(String(255), nullable=True)


class LectureLecturerRecord(Base):
    __tablename__ = "lecture_lecturers"

    lecture_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("lecture_data.lecture_id", ondelete="CASCADE"),
        primary_key=True,
    )
    lecturer_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("lecturer_data.lecturer_id", ondelete="CASCADE"),
        primary_key=True,
    )


class VttFileRecord(Base):
    __tablename__ = "vtt_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    lecture_id: Mapped[int] = mapped_column(Integer, nullable=False)
    language: Mapped[str] = mapped_column(String(50), nullable=False)
    is_original_lang: Mapped[bool] = mapped_column(Boolean, nullable=False)
    vtt_data: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    txt_data: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    asr_model: Mapped[str | None] = mapped_column(String(255), nullable=True)
    compute_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    creation_date: Mapped[dt_datetime | None] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (Index("idx_vtt_files_lecture_id", "lecture_id"),)


class VttLineRecord(Base):
    __tablename__ = "vtt_lines"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    vtt_file_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("vtt_files.id", ondelete="CASCADE"), nullable=False
    )
    series_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("series_data.series_id", ondelete="CASCADE"), nullable=False
    )
    language: Mapped[str] = mapped_column(String(50), nullable=False)
    lecturer_ids: Mapped[list[int]] = mapped_column(ARRAY(Integer), nullable=False)
    line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    ts_start: Mapped[int] = mapped_column(Integer, nullable=False)
    ts_end: Mapped[int] = mapped_column(Integer, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (
        Index(
            "idx_lines_trgm",
            "content",
            postgresql_using="gin",
            postgresql_ops={"content": "gin_trgm_ops"},
        ),
        Index("idx_lines_series_id", "series_id"),
        Index("idx_lines_lecture_id", "vtt_file_id"),
        Index("idx_lines_language", "language"),
    )


class ApiKeyRecord(Base):
    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    api_key: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    person_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    person_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    creation_date: Mapped[dt_datetime | None] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    expiration_date: Mapped[dt_datetime | None] = mapped_column(
        DateTime(timezone=True), server_default=text("(NOW() + INTERVAL '3 months')")
    )
    status: Mapped[str | None] = mapped_column(
        String(255), server_default=text("'active'")
    )

    __table_args__ = (Index("idx_api_keys_api_key", "api_key"),)


class BlacklistIdRecord(Base):
    __tablename__ = "blacklist_ids"

    lecture_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    times_tried: Mapped[int | None] = mapped_column(Integer, server_default=text("1"))
    creation_date: Mapped[dt_datetime | None] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
