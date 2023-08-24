import pathlib
import warnings

from mergify_engine.console_scripts import devtools_cli
from mergify_engine.tests import utils


def test_generate_openapi_spec(tmp_path: pathlib.Path) -> None:
    public_openapi_file = tmp_path / "openapi-public.json"
    result = utils.test_console_scripts(
        devtools_cli.devtools_cli,
        ["generate-openapi-spec", "--visibility", "public", str(public_openapi_file)],
    )
    assert result.exit_code == 0, result.output
    assert result.output == f"{public_openapi_file} created\n"

    internal_openapi_file = tmp_path / "openapi.json"
    with warnings.catch_warnings():
        # Ignore duplicate route warning
        warnings.simplefilter("ignore", category=UserWarning)
        result = utils.test_console_scripts(
            devtools_cli.devtools_cli,
            [
                "generate-openapi-spec",
                "--visibility",
                "internal",
                str(internal_openapi_file),
            ],
        )
    assert result.exit_code == 0, result.output
    assert result.output == f"{internal_openapi_file} created\n"

    # just nesure internal is bigger than public
    assert 0 < public_openapi_file.stat().st_size < internal_openapi_file.stat().st_size
