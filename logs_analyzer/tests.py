from log_analyzer import (xmean, xmedian, fresh_log, open_log)
import unittest
import os

# mock config file
config = {
    "report_size": 1000,
    "report_dir": "./reports",
    "log_dir": "./server_logs",
    "logging": "./logs/log_analizer.log",
    "error_limit": 0.5,
    "template": "./config/report.html",
    "test_log": "nginx-access-ui.log-20170630.gz"
}


class TestSuite(unittest.TestCase):

    def setUp(self):
        self.log_dir = config["log_dir"]
        self.report_dir = config["report_dir"]
        self.name = "(?<=nginx-access-ui\.log-)\d+"
        self.extension = "(?<=\d{8}).*"
        self.filedict = {
            "ok": {"extension": ".gz", "filepath": os.path.join(self.log_dir, "nginx-access-ui.log-20170630.gz")},
            "plain_text": {"extension": "", "filepath":  os.path.join(self.log_dir, "nginx-access-ui.log-201706219")},
            "error": {"extension": "", "filepath": ""},
        }

    def tearDown(self):
        pass

    def test_xmean(self):
        self.assertEqual(xmean([1, 2, 3]), 2.0)
        self.assertEqual(xmean([]), 0.0)

    def test_xmedian(self):
        self.assertEqual(xmedian([1, 2, 3, 4]), 2.5)
        self.assertEqual(xmedian([1, 2, 3, 4, 5]), 3)

    def test_fresh_log(self):
        self.assertFalse(fresh_log(logpath=self.log_dir,
                                   reportpath=self.report_dir,
                                   name_p=self.name,
                                   ext_p=self.extension))
        self.assertIsInstance(fresh_log(logpath=self.log_dir,
                                        reportpath=self.report_dir,
                                        name_p=self.name,
                                        ext_p=self.extension), bool)

    def test_open_log(self):
        self.assertTrue(hasattr(open_log(self.filedict["ok"]), "read"))
        self.assertTrue(hasattr(open_log(self.filedict["plain_text"]), "read"))
        # self.assertRaises(open_log(self.filedict["error"]), IOError)


if __name__ == "__main__":
    unittest.main()
