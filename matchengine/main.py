from __future__ import annotations

import argparse
import os
from multiprocessing import cpu_count

from matchengine.internals.engine import MatchEngine
from matchengine.internals.load import load


def main(run_args):
    """
    Main function which triggers run of engine with args passed in from command line.
    """
    with MatchEngine(
            plugin_dir=run_args.plugin_dir,
            sample_ids=run_args.samples,
            protocol_nos=run_args.trials,
            match_on_closed=run_args.match_on_closed,
            match_on_deceased=run_args.match_on_deceased,
            debug=run_args.debug,
            num_workers=run_args.workers[0],
            config=run_args.config_path,
            db_name=run_args.db_name,
            match_document_creator_class=run_args.match_document_creator_class,
            db_secrets_class=run_args.db_secrets_class,
            report_all_clinical_reasons=run_args.report_all_clinical_reasons,
            ignore_run_log=run_args.ignore_run_log,
            skip_run_log_entry=run_args.skip_run_log_entry,
            trial_match_collection=run_args.trial_match_collection,
            drop=run_args.drop or run_args.drop_and_exit,
            drop_accept=run_args.confirm_drop,
            exit_after_drop=run_args.drop_and_exit,
            resource_dirs=run_args.extra_resource_dirs,
            bypass_warnings=run_args.bypass_warnings
    ) as me:
        me.get_matches_for_all_trials()
        if not args.dry:
            me.update_all_matches()

        if run_args.csv_output:
            me.create_output_csv()


if __name__ == "__main__":
    param_trials_help = ('Path to your trial data file or a directory containing a file for each trial.'
                         'Default expected format is YML.')
    param_mongo_uri_help = ('Your MongoDB URI. If you do not supply one, for matching, it will default to whatever'
                            ' is set to "MONGO_URI" in your SECRETS.JSON file. This file must be set as an '
                            'environmental variable. For data loading you must specify a URI with a database '
                            'ex: mongodb://localhost:27017/matchminer. '
                            'See https://docs.mongodb.com/manual/reference/connection-string/ for more information.')
    param_clinical_help = 'Path to your clinical file. Default expected format is CSV.'
    param_genomic_help = 'Path to your extended_attributes file. Default expected format is CSV'
    csv_output_help = 'Export a csv file of all trial match results'
    param_trial_format_help = 'File format of input trial data. Default is YML.'
    param_patient_format_help = 'File format of input patient data (both clinical and extended_attributes files). Default is CSV.'

    parser = argparse.ArgumentParser()
    closed_help = 'Match on all closed trials and all suspended steps, arms and doses. Default is to skip.'
    deceased_help = 'Match on deceased patients. Default is to match only on alive patients.'
    dry_help = "Execute a full matching run but do not insert any matches into the DB"
    debug_help = "Enable debug logging"
    config_help = ("Path to the config file. By default will look in config/config.json, but if this class is "
                   "imported, will need to be specified explicitly ")
    db_name_help = ("Specify a custom db name to load trials and/or patient data into. If no value is passed, "
                    "db name will be take from SECRETS_JSON file.")
    run_log_help = "Ignore the run log and run on all specified sample IDs/protocol nos"
    base_dir = os.path.dirname(__file__)
    subp = parser.add_subparsers(help='sub-command help')
    subp_p = subp.add_parser('load', help='Sets up your MongoDB for matching.')
    subp_p.add_argument('-t', dest='trial', default=None, help=param_trials_help)
    subp_p.add_argument('-c', dest='clinical', default=None, help=param_clinical_help)
    subp_p.add_argument('-g', dest='extended_attributes', default=None, help=param_genomic_help)
    subp_p.add_argument('--trial-format', dest='trial_format', default='json', action='store', choices=['yml', 'json'],
                        help=param_trial_format_help)
    subp_p.add_argument('--patient-format', dest='patient_format', default='json', action='store',
                        choices=['csv', 'json'],
                        help=param_patient_format_help)
    subp_p.add_argument('--db', dest='db_name', default='', required=False, help=db_name_help)
    subp_p.add_argument("--plugin-dir", dest="plugin_dir",
                        default=os.path.join(base_dir, "plugins"), help="Location of plugin directory")
    subp_p.set_defaults(func=load)
    subp_p = subp.add_parser('match', help='Match patients to trials.')
    subp_p.add_argument("--trials", nargs="*", type=str, default=None)
    subp_p.add_argument("--samples", nargs="*", type=str, default=None)
    subp_p.add_argument("--match-on-closed", dest="match_on_closed", action="store_true", default=False,
                        help=closed_help)
    subp_p.add_argument("--force", dest="ignore_run_log", action="store_true", default=False, help=run_log_help)
    subp_p.add_argument("--skip-run-log-entry",
                        dest="skip_run_log_entry",
                        action="store_true",
                        default=False,
                        help="Skip creating any run log entries for this run.")
    subp_p.add_argument("--visualize-match-paths", dest="visualize_match_paths", action="store_true", default=False,
                        help="Enable to render images of all match paths")
    subp_p.add_argument("--extra-resource-dirs", nargs="*", type=str, default=None)
    subp_p.add_argument("--fig-dir", dest="fig_dir", default='img', help="Directory to store match path images")
    subp_p.add_argument("--dry-run", dest="dry", action="store_true", default=False, help=dry_help)
    subp_p.add_argument("--debug", dest="debug", action="store_true", default=False, help=debug_help)
    subp_p.add_argument("--config-path", dest="config_path",
                        default=os.path.join(base_dir, "config/dfci_config.json"), help=config_help)
    subp_p.add_argument("--override-plugin-dir", dest="plugin_dir", default=os.path.join(base_dir, "plugins"),
                        help="Location of plugin directory")
    subp_p.add_argument("--match-document-creator", dest="match_document_creator_class",
                        default="DFCITrialMatchDocumentCreator",
                        help="Name of class for creating match documents. Should be located in the plugin directory")
    subp_p.add_argument("--db-secrets-class", dest="db_secrets_class", default="DFCIDBSecrets",
                        help="Name of class for obtaining the DB Secrets. Should be located in the plugin directory")
    subp_p.add_argument("--trial-match-collection", dest="trial_match_collection", default="trial_match",
                        help="Collection to store trial matches")
    subp_p.add_argument("--match-on-deceased-patients", dest="match_on_deceased", action="store_true",
                        help=deceased_help)
    subp_p.add_argument("--report-all-clinical-reasons", dest="report_all_clinical_reasons",
                        action="store_true", default=False,
                        help="Report all clinical match reasons")
    subp_p.add_argument("--drop", dest="drop", action="store_true", default=False,
                        help="Drop trials and samples from args supplier")
    subp_p.add_argument("--drop-and-exit", dest="drop_and_exit", action="store_true", default=False,
                        help="Like --drop, but exits directly after")
    subp_p.add_argument("--drop-confirm", dest="confirm_drop", action="store_true", default=False,
                        help="Confirm you wish --drop; skips confirmation prompt")
    subp_p.add_argument("--bypass-warnings", dest="bypass_warnings", action="store_true", default=False,
                        help="Bypass warnings")
    subp_p.add_argument("--workers", nargs=1, type=int, default=[cpu_count() * 5])
    subp_p.add_argument('--db', dest='db_name', default=None, required=False, help=db_name_help)
    subp_p.add_argument('--o', dest="csv_output", action="store_true", default=False, required=False,
                        help=csv_output_help)
    subp_p.set_defaults(func=main)
    args = parser.parse_args()
    args.func(args)
