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


def is_annotation(toml_file):
    """Returns True if a file is a valid annotation file

    Requirements are:
    - that the file starts with the (exact) string:
    '# omero annotation file'
    - that the dictionnary returned by `toml.load` has the
    'project' and 'user' keys.
    """
    with open(toml_file, "r", encoding="utf-8") as fh:
        if not "# omero annotation file" in fh.readline():
            return False
        ann = toml.load(fh)
        return {"project", "user"}.issubset(ann)


def collect_annotations(base_dir: [Path, list]):
    """Finds annotation files throughout the base_dir directory

    Parameters
    ----------
    base_dir : str or :class:`Path`
        the path to recursively parse to find annotation files

    Returns
    -------
    annotation : list of paths
        paths to the annotation files relative to `base_dir` root

    Notes
    -----
    An annotation file is a `toml` file that starts with the (exact) line:
    '# omero annotation file'
    and contains at least the 'project' and 'user' entries

    """

    base_dir = Path(base_dir).resolve()
    all_tomls = base_dir.glob("**/*.toml")
    # This is relative to base_dir's root
    annotation_tomls = [toml for toml in all_tomls if is_annotation(base_dir / toml)]
    return annotation_tomls


def collect_candidates(base_dir: [str, Path], annotation_tomls: list = None):
    """Finds import candidates from base_dir. Only directories with annotation files are imported

    Parameters
    ----------
    base_dir : str or :class:`Path`
        the path to recursively parse to find import candidates
    annotation_tomls: pd.DataFrame (optional)
        if given, do not search for toml files before importing

    Returns
    -------
    to_annotate : dict
        keys are the paths to import candidates,
        values are the paths to their corresponing annotation file

    """
    base_dir = Path(base_dir).resolve()

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

            if not _is_relative_to(candidate, annotated.parent):
                # This is expected as go accross do the whole grid
                continue
            to_annotate[Path(candidate)] = annotated
            log.debug(f"Matched {candidate} with {annotated}")

    not_imported = set(candidates) - set(to_annotate)
    for candidate in not_imported:
        log.info("File %s will not be imported", candidate)

    return to_annotate


def parse_pair(
    candidate_path: Path,
    annotation_path: Path,
    base_dir: Path,
    new_dataset: bool = False,
):
    """Parses an import candidate and its annotation file to produce an
    annotation & import dictionnary.

    Parameters
    ----------
    candidate_path: Path,
        path to the import candidate (as output by omero import_candidates)
    annotation_path: Path,
        path to the annotation toml
    base_dir: Path,
        base directory from which import happens
    new_dataset: bool (default False)
        whether to create a new dataset or use the most recent dataset of
        the same name

    Returns
    -------
    annotation: dict
        dictionnary suitable to call omero CLI importer. The dictionnary contains
        the content of the toml file plus the keys :

        * "target": import target string (see note bellow)
        * "dataset": dataset name in the DB
        * "fileset": fileset name in the DB
        * "file_path": absolute path to the imported fileset

    See Also
    --------

    omero import targets:
    https://docs.openmicroscopy.org/omero/5.6.2/users/cli/import-target.html

    """
    with open(annotation_path, "r", encoding="utf-8") as fh:
        annotation = toml.load(fh)
    project = annotation["project"]

    dataset_dir = annotation_path.parent
    dataset_parts = dataset_dir.relative_to(base_dir.parent).parts
    fileset_parts = candidate_path.relative_to(dataset_dir).parts

    dataset = "-".join(dataset_parts).replace(" ", "_")
    fileset = "-".join(fileset_parts).replace(" ", "_")
    if " " in project:
        # Add quotes
        project = f'"{project}"'

    # https://docs.openmicroscopy.org/omero/5.6.2/users/cli/import-target.html#importing-to-a-dataset-or-screen
    if new_dataset:
        # always create
        target = f"Project:name:{project}/Dataset:@name:{dataset}"
    else:
        # use most recent dataset with that name
        target = f"Project:name:{project}/Dataset:+name:{dataset}"
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


def create_import_table(
    base_dir: [Path, str],
    out_file: [Path, str] = None,
    to_annotate: dict = None,
    update_dataset: bool = False,
):
    """Creates a pandas DataFrame to be consumed by importer_job.auto_import

    Parameters
    ----------
    base_dir: Path or str
        recursively parse to find annotations and files to annotate
    out_file: Path or str, optional
        if passed, saves the import table to this file
    to_annontate: dict, optional
        (candidate, annotation) pairs as produced by `collect_candidates`
    update_dataset: bool, optional, default False
        if True, no new dataset is created, and the data is appended to the
        newest existing dataset of the same name

    Returns
    -------
    table: pd.DataFrame


    """
    base_dir = Path(base_dir).resolve()
    if to_annotate is None:
        to_annotate = collect_candidates(base_dir)
    table = []
    first = True
    for candidate_path, annotation_path in to_annotate.items():
        new = first and not update_dataset
        entry = parse_pair(candidate_path, annotation_path, base_dir, new_dataset=new)
        table.append(entry)
        first = False

    table = pd.DataFrame.from_records(table)
    if out_file is not None:
        table.to_csv(out_file, sep="\t")

    return table


def _is_relative_to(path, other):
    """Tests if a path is relative to an other"""
    try:
        Path(path).relative_to(other)
    except ValueError:
        return False
    return True
