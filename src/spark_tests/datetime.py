"""Module spark_tests.datetime

Returns a fixed datetime as today or now.
"""
import datetime as dt


class FakeDatetime(dt.datetime):
    """`dt.datetime` surrogate.

    Attributes:
        cls.fake_now: `date.datetime` for now,
            by default is `dt.datetime(2021, 3, 20)`.
    """
    fake_now = dt.datetime(2021, 3, 20)

    @classmethod
    def set_fake_now(cls, fake_now: dt.datetime) -> None:
        """Sets cls.fake_now."""
        cls.fake_now = fake_now

    @classmethod
    def now(cls, tz=None) -> dt.datetime:
        """Returns cls.fake_now."""
        return cls.fake_now

    @classmethod
    def today(cls) -> dt.datetime:
        """Returns cls.fake_now."""
        return cls.fake_now


class FakeDate(dt.date):
    """`dt.date` surrogate.
    """
    @classmethod
    def today(cls) -> dt.date:
        """Returns FakeDatetime.now().date()"""
        return FakeDatetime.now().date()
