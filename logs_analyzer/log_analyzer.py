import os
import logging
import re
import gzip
import sys
import configparser
from argparse import ArgumentParser
from string import Template
from datetime import datetime
import copy

NAME = "(?<=nginx-access-ui\.log-)\d+"
EXT = "(?<=\d{8}).*"


def xmean(vals):
    """
    Calculates arithmetic mean. 
    :param vals: list of numbers.
    """
    if len(vals) > 0:
        return sum(vals)/float(len(vals))
    else:
        logging.error("Zero division occured")
        return 0.0


def xmedian(vals):
    """
    Calculates median value of list of numbers.
    :param vals: list of numbers.   
    """
    xvals = sorted(vals)

    if len(xvals) % 2 == 1:
        return xvals[int((len(xvals)+1)/2-1)]
    else:
        lower = xvals[int(len(xvals)/2-1)]
        upper = xvals[int(len(xvals)/2)]
        return (float(lower + upper)) / 2


def fresh_log(logpath, name_p, ext_p):
    """
    Finds the most recent file in the path.
    :param logpath: config path to log files
    :param name_p: pattern for re to extract date from filename
    :param ext_p: pattern for re to extract extension from filename
    :return:
    """ 
    files = os.listdir(logpath)

    # find latest file
    found_files = []
    compiled = re.compile(name_p)
    for f in files:
        if "nginx-access-ui.log" in f:  # select only log with particular filename
            string_date = compiled.findall(f)[0]
            number_date = datetime.strptime(string_date, "%Y%m%d")
            filepath = os.path.join(logpath, f)
            found_files.append({"date": number_date,
                                "filepath": filepath})
    latest_file = sorted(found_files, key=lambda k: k["date"], reverse=True)[0]

    return latest_file


def check_report(file_candidate, reportpath):
    """
    Checks whether the report has been constructed before
    :param file_candidate: dict with file params to check
    :param reportpath: path to report files
    """
    reports = os.listdir(reportpath)

    # extract dates from report names
    report_dates = []
    for rep in reports:
        p = "(?<=report-).*(?=\.)"
        rep_date = re.findall(p, rep)[0]
        num_rep_date = datetime.strptime(rep_date, "%Y.%m.%d")
        report_dates.append(num_rep_date)

    if file_candidate["date"] in report_dates:
        return False
    else:
        return file_candidate


def open_log(filedict):
    """
    Opens file (.gz or plain) with server logs. Returns file object.
    :param filedict: dict with parameters of required file
    """
    path = filedict["filepath"]

    if ".gz" in filedict["filepath"]:
        log = gzip.open(path, "r")
        return log
    else:
        try:
            log = open(path)
            return log
        except IOError as e:
            logging.error(e)
            raise


def line_parse(log):
    """
    Generator to get url and request time from server log.
    :param log: server log file.
    """
    for line in log:
        p = re.compile("[\d|.]+")
        line_split = str(line).split(" ")
        url = line_split[7]
        request_time = re.match(p, line_split[-1]).group(0)

        yield url, float(request_time)


def make_simple_dict(log):
    """
    creates a simple dict {"url1": [1,2,3], "url2": [2,3,4]} with a list of requests times for each url in log.
    :param log: file with server logs.
    """
    lines = line_parse(log)
    storage = {}
    counter = 0  # for logging purposes
    error_counter = 0

    for line in lines:
        url, time = line[0], line[1]

        # simple format error check (url should look like it)
        if url[0] != "/":
            error_counter += 1

        if url in storage.keys():
            storage[url]['times'].append(time)
        else:
            storage[url] = dict()
            storage[url]['times'] = [time]
        
        counter += 1
        if counter % 100000 == 0:
            logging.info("Line {} has been processed.".format(counter))

    error_rate = error_counter / counter

    return storage, error_rate


def make_stats_dict(mydict):
    """
    Preprocess simple dict with a list of request times into dict with url stats
    :param mydict: dict with requests times.
    """
    newdict = {}
    total_count = 0
    total_time = 0
    for key in mydict.keys():
        values = mydict[key]["times"]
        newdict[key] = {}
        newdict[key]["time_sum"] = sum(values)
        newdict[key]["time_max"] = max(values)
        newdict[key]["time_avg"] = xmean(values)
        newdict[key]["time_med"] = xmedian(values)
        newdict[key]["count"] = len(values)
        total_count += len(values)
        total_time += sum(values)

    for key in newdict.keys():
        newdict[key]["count_perc"] = newdict[key]["count"] / total_count
        newdict[key]["time_perc"] = newdict[key]["time_sum"] / total_time
    return newdict


def pretty_list(d, max_length=1000):
    """
    Makes a convenient list of dicts to convert to json.
    :param d: input dictionary
    :param max_length: number of urls to return
    :return: [{"url": url1, "time_sum": 1, ...}, {"url": url2, "time_sum": 3, ...}]
    """
    pretty = []
    for key in d.keys():
        mini = {}
        mini["url"] = key
        mini["time_sum"] = round(d[key]["time_sum"], 3)
        mini["time_max"] = round(d[key]["time_max"], 3)
        mini["time_avg"] = round(d[key]["time_avg"], 3)
        mini["time_med"] = round(d[key]["time_med"], 3)
        mini["count"] = d[key]["count"]
        mini["count_perc"] = round(100*d[key]["count_perc"], 3)
        mini["time_perc"] = round(100*d[key]["time_perc"], 3)
        pretty.append(mini)
    pretty = sorted(pretty, key=lambda k: k["time_sum"], reverse=True)

    # ensure that if length ois small there are no repeat lines
    if len(pretty) > max_length:
        return pretty[:max_length]
    else:
        return pretty

    
def save_report(templatepath, reportpath, pretty_list, report_date):
    """
    Saves report in .html.

    """
    # read html template
    html = open(templatepath, "r", encoding='utf-8').read()
    s = Template(html)
    subs_dict = {"table_json": pretty_list}
    report = s.safe_substitute(subs_dict)
    try:
        with open(reportpath + "/report-" + datetime.strftime(report_date, "%Y.%m.%d") + ".html", "w") as f:
            f.write(report)
        return True
    except OSError:
        logging.error(e)
        raise
    


def main(config):
    """
    Opens log, calculates statistics, renders .html report, saves report into ./reports folder.
    :return:
    """

    # unpack config
    logpath = config["log_dir"]
    reportpath = config["report_dir"]
    report_size = config["report_size"]
    error_limit = config["error_limit"]
    templatepath = config["template"]

    # parse log file
    fresh = fresh_log(logpath=logpath, name_p=NAME, ext_p=EXT)
    check_result = check_report(file_candidate=fresh, reportpath=reportpath)
    logging.info("File has been read.")
    # ensure there is no report for the fresh log file
    if not check_result:
        logging.info("Report has been constructed before. Check ./reports folder.")
        sys.exit(0)
    
    log = open_log(check_result)
    storage, error_rate = make_simple_dict(log)
    logging.info("Simple dictionary has been constructed.")

    if error_rate > float(error_limit):
        logging.info("Parsing error rate is too high (%d%%) " % round(100*error_rate, 3))
        sys.exit(0)
    else:
        logging.info("Parsing error rate is (%d%%) " % round(100*error_rate, 3))

    # calculate statistics
    stats_dict = make_stats_dict(storage)
    logging.info("Statistics dictionary has been constructed.")

    pretty = pretty_list(stats_dict, max_length=report_size)

    # save to html
    written = save_report(templatepath, reportpath, pretty, report_date=check_result["date"])

    if written:
        logging.info("Report has been successfully constructed.")
    


if __name__ == "__main__":

    # patterns for log file
    NAME = "(?<=nginx-access-ui\.log-)\d+"
    EXT = "(?<=\d{8}).*"

    # dealing with configs
    DEFAULT_CONF = {
        "report_size": 1000,
        "report_dir": "./reports",
        "log_dir": "./server_logs",
        "logging": "./logs/log_analizer.log",
        "error_limit": 0.5,
        "template": "./config/report.html"
    }

    default_config = copy.deepcopy(DEFAULT_CONF)

    parser = ArgumentParser()
    parser.add_argument(
        "-config", 
        "--config", 
        dest="config", 
        help="Open specified file",
        default=default_config)

    args = parser.parse_args()

    if args.config:
        default_config.update(args.config)

    # logging depending on config
    logging_destination = default_config.get("logging", "./logs/log_analizer.log")
    logging.basicConfig(format="[%(asctime)s] %(levelname).1s %(message)s:%(lineno)d",
                        level=logging.DEBUG,
                        filename=logging_destination)

    try:
        import cProfile

        cProfile.run(
            '''main(config=default_config)'''
        )

    except KeyboardInterrupt as k:
         logging.exception(k)
         raise
    except Exception as e:
         logging.error("Some unforseen error. %s" % e, exc_info=True)
