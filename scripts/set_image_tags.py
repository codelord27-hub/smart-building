#!/usr/bin/env python3
"""Updates image names/tags in k8s/apps/kustomization.yaml (used by CI, equivalent to
`kustomize edit set image`). Usage: set_image_tags.py <image_repository> <tag> [service ...]"""
import pathlib
import re
import sys

KUSTOMIZATION = pathlib.Path(__file__).resolve().parents[1] / "k8s" / "apps" / "kustomization.yaml"


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    repo, tag, only = sys.argv[1].rstrip("/"), sys.argv[2], set(sys.argv[3:])
    text = KUSTOMIZATION.read_text()
    pattern = re.compile(r"(  - name: (?P<name>[a-z0-9-]+)\n    newName: )(?P<new>[^\n]+)(\n    newTag: )(?P<tag>[^\n]+)")

    def repl(m):
        if only and m["name"] not in only:
            return m.group(0)
        return f"{m.group(1)}{repo}/{m['name']}{m.group(4)}{tag}"

    new_text, n = pattern.subn(repl, text)
    if n == 0:
        sys.exit("no images found in kustomization.yaml")
    KUSTOMIZATION.write_text(new_text)
    print(f"updated {n if not only else len(only)} image(s) -> {repo}/<service>:{tag}")


if __name__ == "__main__":
    main()
