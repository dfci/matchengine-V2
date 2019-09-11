import datetime
import json
import os
from unittest import TestCase

from bson import ObjectId

from matchengine.internals.database_connectivity.mongo_connection import MongoDBConnection
from matchengine.internals.engine import MatchEngine
from matchengine.tests.timetravel_and_override import unoverride_datetime, set_static_date_time


class RunLogTest(TestCase):
    """
    The run_log is a log which keeps track of protocols and sample ID's used by the engine
    in previous runs, by protocol. There are four sources used to determine if any trial and/or
    sample should be updated during any given matchengine run. Those sources are the:
        (1) run_log,
        (2) trial _updated fields
        (3) clinical _updated fields,
        (4) clinical_run_history_trial_match

    Running and updating only the necessary trials and patients is the default behavior of the
    matchengine unless otherwise specified through a CLI flag. These tests enumerate many
    possible combinations of trial and/or patient data changes, and the subsequent expected states
    of the trial_match collection as the matchengine is run on changing and updated data.

    It is assumed that if a patient's genomic document is updated or added, the corresponding
    clinical document's _updated date is updated as well.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.first_run_done = False

    def _reset(self, **kwargs):
        if not self.first_run_done:
            if kwargs.get('do_reset_time', True):
                set_static_date_time()
            self.first_run_done = True
        with MongoDBConnection(read_only=False, db='integration', async_init=False) as setup_db:
            assert setup_db.name == 'integration'

            if not kwargs.get("skip_sample_id_reset", False):
                setup_db.clinical.update({"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
                                         {"$set": {"ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Non-Small Cell Lung Cancer",
                                                   "_updated": datetime.datetime(2001, 1, 1, 1, 1, 1, 1)}})

            if not kwargs.get("skip_vital_status_reset", False):
                setup_db.clinical.update({"SAMPLE_ID": "5d2799da6756630d8dd066a6"},
                                         {"$set": {"VITAL_STATUS": "alive",
                                                   "_updated": datetime.datetime(2001, 1, 1, 1, 1, 1, 1)}})

            if kwargs.get('do_reset_trial_matches', False):
                setup_db.trial_match.drop()

            if kwargs.get('reset_run_log', False):
                setup_db.run_log_trial_match.drop()

            if kwargs.get('do_reset_trials', False):
                setup_db.trial.drop()
                trials_to_load = map(lambda x: os.path.join('matchengine',
                                                            'tests',
                                                            'data',
                                                            'integration_trials',
                                                            x + '.json'),
                                     kwargs.get('trials_to_load', list()))
                for trial_path in trials_to_load:
                    with open(trial_path) as trial_file_handle:
                        trial = json.load(trial_file_handle)
                    setup_db.trial.insert(trial)
            if kwargs.get('do_rm_clinical_run_history', False):
                setup_db.clinical_run_history_trial_match.drop()

        if hasattr(self, 'me'):
            self.me.__exit__(None, None, None)

        self.me = MatchEngine(
            match_on_deceased=kwargs.get('match_on_deceased', True),
            match_on_closed=kwargs.get('match_on_closed', True),
            num_workers=kwargs.get('num_workers', 1),
            visualize_match_paths=kwargs.get('visualize_match_paths', False),
            config=kwargs.get('config', 'matchengine/config/dfci_config.json'),
            plugin_dir=kwargs.get('plugin_dir', 'matchengine/plugins/'),
            match_document_creator_class=kwargs.get('match_document_creator_class', "DFCITrialMatchDocumentCreator"),
            fig_dir=kwargs.get('fig_dir', '/tmp/'),
            protocol_nos=kwargs.get('protocol_nos', None),
            sample_ids=kwargs.get('sample_ids', None),
            report_all_clinical_reasons=kwargs.get("report_all_clinical", True)
        )

        assert self.me.db_rw.name == 'integration'
        # Because ages are relative (people get older with the passage of time :/) the test data will stop working
        # to negate this, we need datetime.datetime.now() and datetime.date.today() to always return the same value
        # To accomplish this, there are overridden classes for datetime.datetime and datetime.date, implementing
        # static versions of now() and today(), respectively

        # The logic for overriding classes is generified here for future extensibility.

        # To perform the override, we first iterate over each of the override classes (at the time of writing,
        # this is just StaticDatetime and StaticDate
        if kwargs.get("do_reset_time", True):
            if kwargs.get('date_args', False):
                set_static_date_time(**kwargs['date_args'])
            else:
                set_static_date_time()
        if kwargs.get("unreplace_dt", False):
            unoverride_datetime()

    def test_run_log_1(self):
        """
        Updated sample, updated curation, trial matches before, trial matches after, but different hashes
        :return:
        """

        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            do_rm_clinical_run_history=True,
            trials_to_load=['run_log_arm_closed'],
            reset_run_log=True,
            match_on_closed=False,
            match_on_deceased=False,
            report_all_clinical=False,
            skip_vital_status_reset=False,
        )
        assert self.me.db_rw.name == 'integration'
        self.me.db_rw.clinical.update({"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
                                      {"$set": {"ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Gibberish",
                                                "_updated": datetime.datetime.now()}})
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find())
        clinical_run_history_trial_match = list(self.me.db_ro.clinical_run_history_trial_match.find())
        assert len(list(self.me.db_ro.trial_match.find({"clinical_id": ObjectId("5d3778bf4fbf195d68cdf4d5")}))) == 0
        assert len(trial_matches) == 0
        assert len(run_log_trial_match) == 1
        assert len(clinical_run_history_trial_match) == 1392

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=True,
            trials_to_load=["run_log_arm_open"],
            reset_run_log=False,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=False
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        clinical_run_history_trial_match = list(
            self.me.db_ro.clinical_run_history_trial_match.find({'clinical_id': ObjectId("5d2799d86756630d8dd065b8")})
        )[0]
        assert len(trial_matches) == 3
        assert len(run_log_trial_match) == 2
        assert len(clinical_run_history_trial_match['run_history']) == 2
        assert len(list(self.me.db_ro.trial_match.find({"clinical_id": ObjectId("5d3778bf4fbf195d68cdf4d5")}))) == 0
        self.me.db_rw.clinical.update({"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
                                      {"$set": {"ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Lung Adenocarcinoma",
                                                "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1)}})

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            trials_to_load=["run_log_arm_open_criteria_change"],
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=True
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        clinical_run_history_trial_match = list(
            self.me.db_ro.clinical_run_history_trial_match.find({'clinical_id': ObjectId("5d2799d86756630d8dd065b8")})
        )[0]
        assert len(trial_matches) == 4
        assert len(disabled_trial_matches) == 1
        assert len(run_log_trial_match) == 3
        assert len(list(self.me.db_ro.trial_match.find({"clinical_id": ObjectId("5d3778bf4fbf195d68cdf4d5")}))) == 0
        assert len(clinical_run_history_trial_match['run_history']) == 3

    def test_run_log_2(self):
        """
        Updated sample, updated curation, trial matches after, but not before
        :return:
        """

        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['run_log_arm_closed'],
            reset_run_log=True,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=True,
            report_all_clinical=False
        )
        assert self.me.db_rw.name == 'integration'
        self.me.db_rw.clinical.update({"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
                                      {"$set": {"ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Gibberish",
                                                "_updated": datetime.datetime.now()}})
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find())
        clinical_run_history_trial_match = list(self.me.db_ro.clinical_run_history_trial_match.find())
        assert len(list(self.me.db_ro.trial_match.find({"clinical_id": ObjectId("5d3778bf4fbf195d68cdf4d5")}))) == 0
        assert len(trial_matches) == 0
        assert len(run_log_trial_match) == 1
        assert len(clinical_run_history_trial_match) == 1392
        self.me.db_rw.clinical.update({"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
                                      {"$set": {"ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Medullary Carcinoma of the Colon",
                                                "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1)}})

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=True,
            trials_to_load=["run_log_arm_open"],
            reset_run_log=False,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=True
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        clinical_run_history_trial_match = list(
            self.me.db_ro.clinical_run_history_trial_match.find({'clinical_id': ObjectId("5d2799d86756630d8dd065b8")})
        )[0]
        assert len(trial_matches) == 2
        assert len(run_log_trial_match) == 2
        assert len(clinical_run_history_trial_match['run_history']) == 2
        assert len(list(self.me.db_ro.trial_match.find({"clinical_id": ObjectId("5d3778bf4fbf195d68cdf4d5")}))) == 0

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            trials_to_load=["run_log_arm_open_criteria_change"],
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=False
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        clinical_run_history_trial_match = list(
            self.me.db_ro.clinical_run_history_trial_match.find({'clinical_id': ObjectId("5d2799d86756630d8dd065b8")})
        )[0]
        assert len(trial_matches) == 3
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 3
        assert len(list(self.me.db_ro.trial_match.find({"clinical_id": ObjectId("5d3778bf4fbf195d68cdf4d5")}))) == 0
        assert len(clinical_run_history_trial_match['run_history']) == 3

    def test_run_log_3(self):
        """
        Updated sample leads to new trial match
        Existing sample not updated does not cause new trial matches
        Sample that doesn't match never matches
        :return:
        """

        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['run_log_arm_closed'],
            reset_run_log=True,
            match_on_closed=True,
            match_on_deceased=False,
            do_rm_clinical_run_history=True,
            report_all_clinical=False
        )
        assert self.me.db_rw.name == 'integration'
        self.me.db_rw.clinical.update({"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
                                      {"$set": {"ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Gibberish",
                                                "_updated": datetime.datetime.now()}})
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find())
        clinical_run_history_trial_match = list(self.me.db_ro.clinical_run_history_trial_match.find())
        assert len(list(self.me.db_ro.trial_match.find({"clinical_id": ObjectId("5d3778bf4fbf195d68cdf4d5")}))) == 0
        assert len(trial_matches) == 2
        assert len(run_log_trial_match) == 1
        assert len(clinical_run_history_trial_match) == 1392

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            match_on_closed=True,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=False
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        clinical_run_history_trial_match = list(
            self.me.db_ro.clinical_run_history_trial_match.find({'clinical_id': ObjectId("5d2799d86756630d8dd065b8")})
        )[0]
        assert len(trial_matches) == 3
        assert len(run_log_trial_match) == 2
        assert len(clinical_run_history_trial_match['run_history']) == 2
        assert len(list(self.me.db_ro.trial_match.find({"clinical_id": ObjectId("5d3778bf4fbf195d68cdf4d5")}))) == 0

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            match_on_closed=True,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=False
        )
        self.me.db_rw.clinical.update({"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
                                      {"$set": {"ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Gibberish",
                                                "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1)}})

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        clinical_run_history_trial_match = list(
            self.me.db_ro.clinical_run_history_trial_match.find({'clinical_id': ObjectId("5d2799d86756630d8dd065b8")})
        )[0]
        assert len(trial_matches) == 3
        assert len(disabled_trial_matches) == 1
        assert len(run_log_trial_match) == 3
        assert len(list(self.me.db_ro.trial_match.find({"clinical_id": ObjectId("5d3778bf4fbf195d68cdf4d5")}))) == 0
        assert len(clinical_run_history_trial_match['run_history']) == 3

    def test_run_log_4(self):
        """
        Update a trial field not used in matching.
        Samples who have matches should continue to have matches.
        Samples without matches should still not have matches.
        :return:
        """
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['run_log_arm_open'],
            reset_run_log=True,
            match_on_closed=True,
            match_on_deceased=False,
            do_rm_clinical_run_history=True,
            report_all_clinical=False
        )
        assert self.me.db_rw.name == 'integration'

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        non_match = list(self.me.db_rw.trial_match.find({"sample_id": ObjectId("5d2799df6756630d8dd068bc")}))
        assert len(trial_matches) == 3
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 1
        assert len(non_match) == 0

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            match_on_closed=True,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=False
        )

        self.me.db_rw.trial.update({"protocol_no": "10-007"},
                                   {"$set": {"unused_field": "ricky_bobby",
                                             "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1)
                                             }})
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        non_match = list(self.me.db_rw.trial_match.find({"sample_id": ObjectId("5d2799df6756630d8dd068bc")}))
        assert len(trial_matches) == 3
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 2
        assert len(non_match) == 0

    def test_run_log_5(self):
        """
        Update a trial arm status field. Update a sample.
        After update sample with matches should continue to have matches.
        After update sample without matches should still not have matches.
        :return:
        """
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['run_log_two_arms'],
            reset_run_log=True,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=True,
            report_all_clinical=False
        )
        assert self.me.db_rw.name == 'integration'

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        non_match = list(self.me.db_rw.trial_match.find({"sample_id": ObjectId("5d2799df6756630d8dd068bc")}))
        assert len(trial_matches) == 3
        for match in trial_matches:
            assert match['internal_id'] == 101
            assert match['is_disabled'] == False
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 1
        assert len(non_match) == 0

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=False
        )

        self.me.db_rw.trial.update({"protocol_no": "10-007"},
                                   {"$set": {"treatment_list.step.0.arm.1.arm_suspended": "N",
                                             "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1)
                                             }})
        # update non-match
        self.me.db_rw.clinical.update({"SAMPLE_ID": "5d2799df6756630d8dd068bb"},
                                      {"$set": {"ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Gibberish",
                                                "_updated": datetime.datetime.now()}})

        # update matching
        self.me.db_rw.genomic.insert({
            "SAMPLE_ID": "5d2799da6756630d8dd066a6",
            "clinical_id": ObjectId("5d2799da6756630d8dd066a6"),
            "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1),
            "TRUE_HUGO_SYMBOL": "sonic_the_hedgehog"
        })

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        non_match = list(self.me.db_rw.trial_match.find({"sample_id": ObjectId("5d2799df6756630d8dd068bc")}))
        assert len(trial_matches) == 3
        for match in trial_matches:
            assert match['internal_id'] == 101
            assert match['is_disabled'] == False
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 2
        assert len(non_match) == 0

        self.me.db_rw.genomic.remove({"TRUE_HUGO_SYMBOL": "sonic_the_hedgehog"})

    def test_run_log_6(self):
        """
        Update a trial arm status field.
        Update a sample's vital_status to deceased.
        Sample should no longer have matches.
        :return:
        """
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['run_log_two_arms'],
            reset_run_log=True,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=True,
            report_all_clinical=False
        )
        assert self.me.db_rw.name == 'integration'

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(enabled_trial_matches) == 3
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 1

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=False
        )

        self.me.db_rw.trial.update({"protocol_no": "10-007"},
                                   {"$set": {"treatment_list.step.0.arm.1.arm_suspended": "N",
                                             "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1)
                                             }})

        self.me.db_rw.clinical.update({"SAMPLE_ID": "5d2799da6756630d8dd066a6"},
                                      {"$set": {"VITAL_STATUS": "deceased",
                                                "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1)
                                                }})

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(enabled_trial_matches) == 3
        for match in disabled_trial_matches:
            assert match['sample_id'] == "5d2799da6756630d8dd066a6"
        assert len(disabled_trial_matches) == 2
        assert len(run_log_trial_match) == 2

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=False
        )

        self.me.db_rw.trial.update({"protocol_no": "10-007"},
                                   {"$set": {"unused_field": "ricky_bobby",
                                             "_updated": datetime.datetime(2002, 2, 1, 1, 1, 1, 1)
                                             }})

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(enabled_trial_matches) == 3
        for match in disabled_trial_matches:
            assert match['sample_id'] == "5d2799da6756630d8dd066a6"
        assert len(disabled_trial_matches) == 2
        assert len(run_log_trial_match) == 3

    def test_run_log_7(self):
        """
        Update a trial curation.
        Update a sample's vital_status to deceased.
        Sample should no longer have matches.
        :return:
        """
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['run_log_two_arms'],
            reset_run_log=True,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=True,
            report_all_clinical=False
        )
        assert self.me.db_rw.name == 'integration'

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(trial_matches) == 3
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 1

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=False
        )

        self.me.db_rw.trial.update({"protocol_no": "10-007"},
                                   {"$set": {"treatment_list.step.0.arm.0.match.0.and.0.hugo_symbol": "BRAF",
                                             "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1)}})

        self.me.db_rw.clinical.update({"SAMPLE_ID": "5d2799da6756630d8dd066a6"},
                                      {"$set": {"VITAL_STATUS": "deceased",
                                                "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1)}})

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(trial_matches) == 5
        for match in disabled_trial_matches:
            assert match['sample_id'] == "5d2799da6756630d8dd066a6"
        assert len(disabled_trial_matches) == 2
        assert len(run_log_trial_match) == 2

    def test_run_log_8(self):
        """
        Update a sample's vital_status to deceased.
        Sample should have matches before run and not after.
        :return:
        """
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['run_log_arm_open'],
            reset_run_log=True,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=True,
            report_all_clinical=False
        )
        assert self.me.db_rw.name == 'integration'

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(enabled_trial_matches) == 3
        sample_count = 0
        for match in enabled_trial_matches:
            if match['sample_id'] == "5d2799da6756630d8dd066a6":
                sample_count += 1
        assert sample_count == 2
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 1

        self.me.db_rw.clinical.update({"SAMPLE_ID": "5d2799da6756630d8dd066a6"},
                                      {"$set": {"VITAL_STATUS": "deceased",
                                                "_updated": datetime.datetime(2002, 2, 1, 1, 1, 1, 1)}})

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=False,
            skip_vital_status_reset=True
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        assert len(enabled_trial_matches) == 1
        for match in enabled_trial_matches:
            assert match['sample_id'] != "5d2799da6756630d8dd066a6"
        for match in disabled_trial_matches:
            assert match['sample_id'] == "5d2799da6756630d8dd066a6"
        assert len(disabled_trial_matches) == 2
        assert len(run_log_trial_match) == 2

        self.me.db_rw.clinical.update({"SAMPLE_ID": "5d2799da6756630d8dd066a6"},
                                      {"$set": {"VITAL_STATUS": "alive",
                                                "_updated": datetime.datetime(2002, 2, 1, 1, 1, 1, 1)}})

    def test_run_log_9(self):
        """
        Update a trial arm status to open.
        Run on a new sample.
        Sample should have matches.
        Sample which doesn't match should still not match
        :return:
        """
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['all_closed'],
            reset_run_log=True,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=True,
            report_all_clinical=False
        )
        assert self.me.db_rw.name == 'integration'

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        no_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799df6756630d8dd068ba"}))
        known_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799cc6756630d8dd06265"}))
        assert len(enabled_trial_matches) == 0
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 1
        assert len(no_match) == 0
        assert len(known_match) == 0

        self.me.db_rw.trial.update({"protocol_no": "10-001"},
                                   {"$set": {"treatment_list.step.0.arm.0.arm_suspended": "N",
                                             "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1)
                                             }})

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=False
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        no_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799df6756630d8dd068ba"}))
        known_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799cc6756630d8dd06265"}))
        assert len(enabled_trial_matches) == 8
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 2
        assert len(no_match) == 0
        assert len(known_match) == 1

    def test_run_log_10(self):
        """
        Update a trial field not used in matching.
        Run on a new sample.
        Sample should have matches.
        Sample which doesn't match should still not match.
        :return:
        """
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['all_open'],
            reset_run_log=True,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=True,
            report_all_clinical=False
        )
        assert self.me.db_rw.name == 'integration'

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        no_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799df6756630d8dd068ba"}))
        assert len(enabled_trial_matches) == 8
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 1
        assert len(no_match) == 0

        self.me.db_rw.trial.update({"protocol_no": "10-001"},
                                   {"$set": {"unused_field": "ricky_bobby",
                                             "_updated": datetime.datetime(2002, 1, 1, 1, 1, 1, 1)
                                             }})

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=False
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        enabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": False}))
        disabled_trial_matches = list(self.me.db_ro.trial_match.find({"is_disabled": True}))
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        no_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799df6756630d8dd068ba"}))
        assert len(enabled_trial_matches) == 8
        assert len(disabled_trial_matches) == 0
        assert len(run_log_trial_match) == 2
        assert len(no_match) == 0

    def test_run_log_11(self):
        """
        Update a sample's vital_status to deceased.
        Sample should not have matches before or after run.
        A third run with no trial changes should not produce matches.
        :return:
        """
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['all_closed'],
            reset_run_log=True,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=True,
            report_all_clinical=False
        )
        assert self.me.db_rw.name == 'integration'

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        no_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799da6756630d8dd066a6"}))
        assert len(no_match) == 0
        assert len(run_log_trial_match) == 1

        self.me.db_rw.clinical.update({"SAMPLE_ID": "5d2799da6756630d8dd066a6"},
                                      {"$set": {"VITAL_STATUS": "deceased",
                                                "_updated": datetime.datetime(2002, 2, 1, 1, 1, 1, 1)}})

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=False,
            skip_vital_status_reset=True
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        no_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799da6756630d8dd066a6"}))
        assert len(no_match) == 0
        assert len(run_log_trial_match) == 2

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            match_on_closed=False,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=False,
            skip_vital_status_reset=True
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        no_match = list(self.me.db_ro.trial_match.find({"sample_id": "5d2799da6756630d8dd066a6"}))
        assert len(no_match) == 0
        assert len(run_log_trial_match) == 3

    def tearDown(self) -> None:
        if hasattr(self, 'me'):
            self.me.__exit__(None, None, None)
