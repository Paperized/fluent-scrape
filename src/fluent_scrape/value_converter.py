import traceback
from typing import Any, Protocol
from datetime import datetime


class XValueConverterCallback(Protocol):
    def __call__(self, value: str | Any, *args) -> Any:
        ...


def __to_string__(value: Any) -> str:
    return str(value)


def __to_int__(value: Any) -> int:
    value = str(value).replace(",", "").replace(".", "")
    return int(value)


def __to_float__(value: Any) -> float:
    value = str(value).replace(",", ".")
    return float(value)


def __to_bool__(value: Any) -> bool:
    return bool(value)


def __to_datetime__(value: Any, *args) -> datetime:
    return datetime.strptime(value, args[0])


class XValueConverter(object):
    table: dict[str, XValueConverterCallback] = {
        "str": __to_string__,
        "int": __to_int__,
        "float": __to_float__,
        "bool": __to_bool__,
        "datetime": __to_datetime__
    }

    @staticmethod
    def register_type(name: str, func: XValueConverterCallback):
        XValueConverter.table[name] = func

    @staticmethod
    def convert(value: str | Any, type_name: str = "str", *vargs) -> Any:
        debug = type_name.endswith("_or_noneD")
        none_if_exception = debug or type_name.endswith("_or_none")

        converter_name = type_name if not none_if_exception else (type_name[:-8] if not debug else type_name[:-9])
        if converter_name in XValueConverter.table:
            if not none_if_exception:
                return XValueConverter.table[converter_name](value, *vargs)

            try:
                return XValueConverter.table[converter_name](value, *vargs)
            except:
                if debug:
                    traceback.print_exc()
                return None
        else:
            raise Exception(f"Unknown type: {type_name}")
