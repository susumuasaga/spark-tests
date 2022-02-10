"""Module spark_tests.datetime

Return a fixed datetime as today or now.
"""
import datetime as dt


class FakeDatetime(dt.datetime):
    """`dt.datetime` surrogate.

    Attributes:
        cls.fake_now: `FakeDatetime` for now,
            by default is `dt.datetime`(2021, 3, 20)
    """
    fake_now = dt.datetime(2021, 3, 20)

    @classmethod
    def set_fake_now(cls, fake_now: dt.datetime) -> None:
        """Set cls.fake_now."""
        cls.fake_now = fake_now

    @classmethod
    def now(cls, tz=None) -> dt.datetime:
        """Return cls.fake_now."""
        return cls.fake_now

    @classmethod
    def today(cls) -> dt.datetime:
        """Return cls.fake_now."""
        return cls.fake_now


class FakeDate(dt.date):
    """`dt.date` surrogate.

    Attributes:
        fake_today: fake `dt.date` for today,
            by default is `dt.date`(2021, 3, 20)
    """
    fake_today = dt.date(2021, 3, 20)

    @classmethod
    def set_fake_today(cls, fake_today: dt.date) -> None:
        """Set cls.fake_today"""
        cls.fake_today = fake_today

    @classmethod
    def today(cls) -> dt.date:
        """Return cls.fake_today"""
        return cls.fake_today
