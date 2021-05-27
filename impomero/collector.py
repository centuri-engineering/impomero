import os
import logging
from pathlib import Path
from datetime import date

import toml
import pandas as pd
from omero.util import import_candidates

log = logging.getLogger(__name__)


def get_configuration():
    """Get configuration from environement

    Returns a configuration dictionnary

    """
    conf = {}
    env = os.environ
    conf["server"] = env.get("OMERO_SERVER", "localhost")
    conf["port"] = env.get("OMERO_PORT", "4064")
    conf["admin_passwd"] = env.get("OMERO_ROOT_PASSWORD")
    return conf


def _has_annotation(directory):
    tomls = Path(directory).glob("*.toml")
    if not tomls:
        return False
    for toml_file in tomls:
        if _is_annotation(toml_file):
            return toml_file
    return False


def _is_annotation(toml_file):
    with open(toml_file, "r", encoding="utf-8") as fh:
        if not "# omero annotation file" in fh.readline():
            return False
        ann = toml.load(fh)
        return {"project", "user"}.issubset(ann)


def collect_annotations(base_dir):
    """Finds annotation files throughout the base_dir directory"""

    base_dir = Path(base_dir)
    all_tomls = base_dir.glob("**/*.toml")
    # This is relative to base_dir
    annotation_tomls = [
        base_dir / toml for toml in all_tomls if _is_annotation(base_dir / toml)
    ]
    return annotation_tomls


def collect_candidates(base_dir, annotation_tomls=None):

    base_dir = Path(base_dir)

    if annotation_tomls is None:
        annotation_tomls = collect_annotations(base_dir)

    candidates = import_candidates.as_dictionary(base_dir.as_posix())
    log.info("Found %d import candidates", len(candidates))
    candidate_paths = sorted(candidates, key=lambda p: Path(p).parent.as_posix())
    annotated_paths = list(annotation_tomls)

    # sort annotated by depth, deeper first
    annotated_paths.sort(key=lambda p: len(p.parts), reverse=True)
    to_annotate = {}
    for annotated in annotated_paths:
        for candidate in candidate_paths:
            if candidate in to_annotate:
                log.debug(
                    "File %s already annotated by %s", candidate, to_annotate[candidate]
                )
                continue
            try:
                Path(candidate).relative_to(annotated.parent)
            except ValueError:
                continue
            to_annotate[candidate] = annotated
            log.debug(f"Matched {candidate} with {annotated}")

    not_imported = set(candidates) - set(to_annotate)
    for candidate in not_imported:
        log.info("File %s will not be imported", candidate)

    return to_annotate


def parse_pair(candidate_path, annotation_path, base_dir):

    with open(annotation_path, "r") as fh:
        annotation = toml.load(fh)
    project = annotation["project"]

    dataset_dir = annotation_path.parent
    dataset_parts = Path(dataset_dir).relative_to(base_dir.parent).parts
    fileset_parts = Path(candidate_path).relative_to(dataset_dir).parts

    dataset = "-".join(dataset_parts).replace(" ", "_")
    fileset = "-".join(fileset_parts).replace(" ", "_")

    # use most recent dataset with that name
    # https://docs.openmicroscopy.org/omero/5.6.2/users/cli/import-target.html#importing-to-a-dataset-or-screen
    target = f'Project:name:"{project}"/Dataset:+name:"{dataset}"'
    file_path = Path(candidate_path).absolute().as_posix()
    annotation.update(
        {
            "target": target,
            "dataset": dataset,
            "fileset": fileset,
            "file_path": file_path,
        }
    )
    return annotation


def create_import_table(base_dir, out_file=None, to_annotate=None):

    base_dir = Path(base_dir)
    if to_annotate is None:
        to_annotate = collect_candidates(base_dir)

    table = pd.DataFrame.from_records(
        (
            parse_pair(candidate_path, annotation_path, base_dir)
            for candidate_path, annotation_path in to_annotate.items()
        )
    )
    if out_file is not None:
        table.to_csv(out_file, sep="\t")

    return table
