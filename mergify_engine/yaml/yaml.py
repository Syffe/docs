import typing

import yaml

# convenient aliases
from mergify_engine.yaml import yaml_anchor_extractor


def extended_anchors_load(
    stream: typing.Any,
    anchors: dict[str, typing.Any],
) -> typing.Any:
    loader = yaml_anchor_extractor.ExtendedAnchorLoader(stream, anchors=anchors)
    try:
        return loader.get_single_data()
    finally:
        loader.dispose()  # type: ignore[no-untyped-call]


def anchor_extractor_load(stream: typing.Any) -> typing.Any:
    # FIXME(sileht): we should find a way to limit the number of anchors referenced by other part of the document
    return yaml.load(stream, Loader=yaml_anchor_extractor.AnchorExtractorLoader)  # type: ignore[arg-type]


def safe_load(stream: typing.Any) -> typing.Any:
    # FIXME(sileht): we should find a way to limit the number of anchors referenced by other part of the document
    return yaml.load(stream, Loader=yaml.CSafeLoader)


def safe_dump(
    data: typing.Any,
    default_flow_style: bool | None = None,
) -> typing.Any:
    return yaml.dump(
        data,
        Dumper=yaml.CSafeDumper,
        default_flow_style=default_flow_style,
    )


def dump(data: typing.Any) -> typing.Any:
    return yaml.dump(data, Dumper=yaml.CSafeDumper)


# NOTE(sileht): increase_indent can't be customise with CDumper
# NOTE(greesb): Workaround to have corrent indentation on lists
# https://stackoverflow.com/questions/25108581/python-yaml-dump-bad-indentation
class DumperWithIndentedList(yaml.SafeDumper):
    def increase_indent(
        self,
        flow: bool = False,
        *args: typing.Any,  # noqa: ARG002
        **kwargs: typing.Any,  # noqa: ARG002
    ) -> typing.Any:
        return super().increase_indent(flow=flow, indentless=False)


def dump_with_indented_list(data: typing.Any) -> typing.Any:
    return yaml.dump(data, Dumper=DumperWithIndentedList)


class LiteralYamlString(yaml.YAMLObject):
    yaml_tag = ""
    yaml_loader: typing.ClassVar[list[typing.Any]] = []  # type: ignore [misc]
    yaml_dumper = yaml.CSafeDumper

    def __init__(self, data: str) -> None:
        self.data = data

    @classmethod
    def to_yaml(cls, dumper: yaml.BaseDumper, data: typing.Any) -> typing.Any:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data.data, style="|")
