import typing

from yaml import composer
from yaml import constructor
from yaml import nodes
from yaml import parser
from yaml import reader
from yaml import resolver
from yaml import scanner


class AnchorExtractorComposer(composer.Composer):
    def __init__(self) -> None:
        self.anchors = {}
        # NOTE(Syffe): This is the added change
        self.anchors_historic: dict[str, typing.Any] = {}

    def compose_document(self) -> nodes.Node | None:
        # Drop the DOCUMENT-START event.
        self.get_event()  # type: ignore[attr-defined]

        # Compose the root node.
        # NOTE(Syffe): This is a typing bug inside typeshed and yaml, the index argument can be None
        # but is typed as int only
        # see: https://github.com/python/typeshed/blob/main/stubs/PyYAML/yaml/composer.pyi#L15
        # and (where index is the variable current_index) https://github.com/yaml/pyyaml/blob/main/lib/yaml/resolver.py#L129-L132
        node = self.compose_node(None, None)  # type: ignore[arg-type]
        # NOTE(Syffe): This is the added change
        self.anchors_historic.update(self.anchors)

        # Drop the DOCUMENT-END event.
        self.get_event()  # type: ignore[attr-defined]
        self.anchors = {}
        return node


class ExtendedAnchorComposer(composer.Composer):
    # NOTE(Syffe): This is the added change
    def __init__(self, anchors: dict[str, typing.Any]) -> None:
        # NOTE(Syffe): No need to init super class since the init function
        # only contains `self.anchors = {}`
        self.anchors = anchors


class AnchorExtractorConstructor(constructor.SafeConstructor):
    def get_single_data(self) -> tuple[typing.Any, dict[str, typing.Any]] | None:
        # Ensure that the stream contains a single document and construct it.
        node = self.get_single_node()  # type: ignore[attr-defined]
        if node is not None:
            document = self.construct_document(node)  # type: ignore[no-untyped-call]
            # NOTE(Syffe): This is the added change
            return (document, self.anchors_historic)  # type: ignore[attr-defined]
        return None


class AnchorExtractorLoader(
    reader.Reader,
    scanner.Scanner,
    parser.Parser,
    AnchorExtractorComposer,
    AnchorExtractorConstructor,
    resolver.Resolver,
):
    """Loader that extracts parsed anchors from the document"""

    def __init__(self, stream: "reader._ReadStream") -> None:
        reader.Reader.__init__(self, stream)
        scanner.Scanner.__init__(self)
        parser.Parser.__init__(self)
        AnchorExtractorComposer.__init__(self)
        AnchorExtractorConstructor.__init__(self)
        resolver.Resolver.__init__(self)


class ExtendedAnchorLoader(
    reader.Reader,
    scanner.Scanner,
    parser.Parser,
    ExtendedAnchorComposer,
    constructor.SafeConstructor,
    resolver.Resolver,
):
    """Loader uses external anchors to load the document"""

    def __init__(self, stream: "reader._ReadStream", anchors: dict[str, typing.Any]):
        reader.Reader.__init__(self, stream)
        scanner.Scanner.__init__(self)
        parser.Parser.__init__(self)
        ExtendedAnchorComposer.__init__(self, anchors=anchors)
        constructor.SafeConstructor.__init__(self)
        resolver.Resolver.__init__(self)
