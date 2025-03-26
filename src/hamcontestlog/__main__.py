"""Command-line interface."""

import click


@click.command()
@click.version_option()
def main() -> None:
    """HamContestLog."""


if __name__ == "__main__":
    main(prog_name="hamcontestlog")  # pragma: no cover
