from pymethodic.pymethodic import summarising, preprocessing, subsetting
import unittest

class PreprocessingTest(unittest.TestCase):
    def test_no_args(self):
        print("Running preprocessing without arguments")
        preprocessing.preprocess_folder(
            infolder='resources/rawdata',
            outfolder='resources/preprocessed'
        )
        self.assertTrue(True)

    def test_args(self):
        print("Running preprocessing with arguments")
        preprocessing.preprocess_folder(
            infolder='resources/rawdata',
            outfolder='resources/preprocessed',
            precision = 3600,
            sessioninterval = [30]
        )

class SubsettingTest(unittest.TestCase):
    def test_no_args(self):
        print("Running subsetting")
        subsetting.subset(
            infolder='resources/preprocessed',
            outfolder='resources/preprocessed',
            removefile='resources/remove.csv',
            subsetfile='resources/subset.csv',
        )

class SummaryTest(unittest.TestCase):
    def test_no_args(self):
        print("Running summary test without arguments")
        summarising.summary(
                infolder = 'resources/preprocessed',
                outfolder = 'resources/output',
                includestartend = True
            )

    def test_args(self):
        print("Running summary with arguments")
        summarising.summary(
            infolder='resources/preprocessed',
            outfolder='resources/output',
            includestartend=True,
            recodefile='resources/categorisation.csv',
            quarterly=False,
            splitweek=True,
            weekdefinition="weekdayMF",
            splitday=False,
            maxdays=2
        )

if __name__ == '__main__':
    unittest.main()