from unittest import TestCase
from matchengine import MatchEngine


class TestMatchEngine(TestCase):
    base_tree = {
        "and0": [
            {
                "and1": [
                    {
                        "g1": {
                            "g1_key": "g1_val"
                        },
                        "c1": {
                            "c1_key": "c1_val"
                        }
                    },
                    {
                        "or2": [
                            {
                                "g2": {
                                    "g2_key": "g2_val"
                                }
                            },
                            {
                                "c2": {
                                    "c2_key": "c2_val"
                                }
                            },
                            {
                                "and3": [
                                    {
                                        "g3": {
                                            "g3_key": "g3_val"
                                        }
                                    },
                                    {
                                        "c3": {
                                            "c3_key": "c3_val"
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "or4": [
                            {
                                "g4": {
                                    "g4_key": "g4_val"
                                }
                            },
                            {
                                "c4": {
                                    "c4_key": "c4_val"
                                }
                            },
                            {
                                "or5": [
                                    {
                                        "g5": {
                                            "g5_key": "g5_val"
                                        }
                                    },
                                    {
                                        "c5": {
                                            "c5_key": "c5_val"
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "and6": [
                            {
                                "g6": {
                                    "g6_key": "g6_val"
                                },
                                "c6": {
                                    "c6_key": "c6_val"
                                }
                            },
                            {
                                "or7": [
                                    {
                                        "g7": {
                                            "g7_key": "g7_val"
                                        }
                                    },
                                    {
                                        "c7": {
                                            "c7_key": "c7_val"
                                        }
                                    }
                                ]
                            },
                            {
                                "and8": [
                                    {
                                        "g8": {
                                            "g8_key": "g8_val"
                                        }
                                    },
                                    {
                                        "c8": {
                                            "c8_key": "c8_val"
                                        }
                                    }
                                ]
                            }
                        ]
                    }

                ],
                "or9": [
                    {
                        "g9": {
                            "g9_key": "g9_val"
                        },
                        "c9": {
                            "c9_key": "c9_val"
                        }
                    },
                    {
                        "and10": [
                            {
                                "g10": {
                                    "g10_key": "g10_val"
                                },
                                "c10": {
                                    "c10_key": "c10_val"
                                }
                            },
                            {
                                "and11": [
                                    {
                                        "g11": {
                                            "g11_key": "g11_val"
                                        }
                                    },
                                    {
                                        "c11": {
                                            "c11_key": "c11_val"
                                        }
                                    }
                                ]
                            },
                            {
                                "or12": [
                                    {
                                        "g12": {
                                            "g12_key": "g12_val"
                                        }
                                    },
                                    {
                                        "c12": {
                                            "c12_key": "c12_val"
                                        }
                                    }
                                ]
                            }
                        ],
                    },
                    {
                        "or13": [
                            {
                                "g13": {
                                    "g13_key": "g13_val"
                                }
                            },
                            {
                                "c13": {
                                    "g14_key": "g14_val"
                                }
                            },
                            {
                                "or14": [
                                    {
                                        "g14": {
                                            "g14_key": "g14_val"
                                        }
                                    },
                                    {
                                        "c14": {
                                            "c14_key": "c14_val"
                                        }
                                    }
                                ]
                            },
                            {
                                "and15": [
                                    {
                                        "g15": {
                                            "g15_key": "g15_val"
                                        }
                                    },
                                    {
                                        "c15": {
                                            "c15_key": "c15_val"
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }

    def setUp(self) -> None:
        self.matchengine = MatchEngine(db_init=False)
        pass

    def test__async_exit(self):
        self.fail()

    def test__find_plugins(self):
        self.fail()

    def test__async_init(self):
        self.fail()

    def test__execute_clinical_queries(self):
        self.fail()

    def test__execute_genomic_queries(self):
        self.fail()

    def test__run_query(self):
        self.fail()

    def test__queue_worker(self):
        self.fail()

    def test_extract_match_clauses_from_trial(self):
        self.fail()

    def test_create_match_tree(self):
        self.fail()

    def test_get_match_paths(self):
        self.fail()

    def test_translate_match_path(self):
        self.fail()

    def test_update_matches_for_protocol_number(self):
        self.fail()

    def test_update_all_matches(self):
        self.fail()

    def test__async_update_matches_by_protocol_no(self):
        self.fail()

    def test_get_matches_for_all_trials(self):
        self.fail()

    def test_get_matches_for_trial(self):
        self.fail()

    def test__async_get_matches_for_trial(self):
        self.fail()

    def test_get_clinical_ids_from_sample_ids(self):
        self.fail()

    def test_get_trials(self):
        self.fail()

    def test__perform_db_call(self):
        self.fail()

    def test_get_sort_order(self):
        self.fail()

    def test_create_trial_matches(self):
        self.fail()
