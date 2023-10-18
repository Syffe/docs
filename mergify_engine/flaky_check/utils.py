import enum


class NeedRerunStatus(enum.Enum):
    UNKONWN = None
    NEED_RERUN = True
    DONT_NEED_RERUN = False
