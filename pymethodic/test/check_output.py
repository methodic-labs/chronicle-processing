from pymethodic.pymethodic import summarising, preprocessing, subsetting
from argparse import ArgumentParser
import pandas as pd
import unittest
import shutil
import yaml
import os

class PreprocessingOutputTest(unittest.TestCase):

    def test_preprocessing(self):
        print("Test preprocessing simple")
        preprocessing.preprocess_folder(
            infolder='resources/rawdata',
            outfolder='resources/preprocessed'
        )
        self.assertTrue(True)

    def test_output(self):
        print("Test preprocessing output")

        directory = '.'

        # preprocessed
        files = {
            "preprocessing": {
                "expected": os.path.join(
                    directory,
                    'resources/constants/constants_preprocessed.yaml'
                ),
                "encountered": os.path.join(
                    directory,
                    'resources/preprocessed/ChronicleData_preprocessed-TestParticipant.csv'
                ),
                "folder": os.path.join(directory, "resources/preprocessed")
            },
            "summarising": {
                "expected": os.path.join(
                    directory,
                    'resources/constants/constants_summary.yaml'
                ),
                "encountered": os.path.join(
                    directory,
                    'resources/output/summary_daily.csv'
                ),
                "folder": os.path.join(directory, "resources/output")
            }
        }

        for keyword in files.keys():

            print("Testing pipeline for %s"%keyword)

            with open(files[keyword]['expected'], 'r') as outfile:
                expected_output = yaml.safe_load(outfile)

            encountered_output = pd.read_csv(files[keyword]['encountered']).to_dict()
            if not encountered_output == expected_output:
                print("    There are files with different values !!")
                for key, value in expected_output.items():
                    if value != encountered_output[key]:
                        print("        Difference for %s: %s vs. %s"%(key, str(value[0]), str(encountered_output[key][0])))

                raise ValueError("    This isn't right !")
            else:
                print("    Test successful...")
                shutil.rmtree(files[keyword]['folder'])

if __name__ == '__main__':
    unittest.main()
