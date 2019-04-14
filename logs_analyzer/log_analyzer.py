import os
import logging
import re
import gzip
import sys
import configparser
from argparse import ArgumentParser
from string import Template
from datetime import datetime


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


def fresh_log(logpath, reportpath, name_p, ext_p):
    """
    Finds the most recent file in the path.
    :param logpath: config path to log files
    :param reportpath:  config path to report files
    :param name_p: pattern for re to extract date from filename
    :param ext_p: pattern for re to extract extension from filename
    :return:
    """ 
    files = os.listdir(logpath)
    reports = os.listdir(reportpath)

    # extract dates from report names
    report_dates = list()
    for rep in reports:
        p = "(?<=report-).*(?=\.)"
        rep_date = re.findall(p, rep)[0]
        num_rep_date = datetime.strptime(rep_date, "%Y.%m.%d")
        report_dates.append(num_rep_date)

    # find latest file
    lst = list()
    for f in files:
        if "nginx-access-ui.log" in f:  # select only log with particular filename
            string_date = re.compile(name_p).findall(f)[0]
            extension = re.compile(ext_p).findall(f)[0]
            number_date = datetime.strptime(string_date, "%Y%m%d")
            filepath = logpath + "/" + f
            lst.append({"date": number_date,
                        "extension": extension,
                        "filepath": filepath})
    latest_file = sorted(lst, key=lambda k: k["date"], reverse=True)[0]

    # check whether the report has been constructed before
    if latest_file["date"] in report_dates:
        return False
    else:
        return latest_file


def open_log(filedict):
    """
    Opens file (.gz or plain) with server logs. Returns file object.
    :param filedict: dict with parameters of required file
    """
    path = filedict["filepath"]
    extension = filedict["extension"]

    if extension == ".gz":
        log = gzip.open(path, "r")
        return log
    elif extension == "":
        try:
            log = open(path)
            return log
        except IOError as e:
            logging.error(e)
            raise
    else:
        logging.error("File has wrong extension")
        raise IOError


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
    storage = dict()
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
    newdict = dict()
    total_count = 0
    total_time = 0
    for key in mydict.keys():
        values = mydict[key]["times"]
        newdict[key] = dict()
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
    pretty = list()
    for key in d.keys():
        mini = dict()
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


def main(logpath, reportpath, report_size, error_limit, templatepath, name_p, ext_p):
    """
    Opens log, calculates statistics, renders .html report, saves report into ./reports folder.
    :return:
    """
    # parse log file
    f = fresh_log(logpath=logpath, reportpath=reportpath, name_p=name_p, ext_p=ext_p)
    logging.info("File has been read.")
    # ensure there is no report for the fresh log file
    if f:
        log = open_log(f)
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

        # read html template
        html = open(templatepath, "r", encoding='utf-8').read()
        s = Template(html)
        subs_dict = {"table_json": pretty}
        report = s.safe_substitute(subs_dict)
        with open(reportpath + "/report-" + datetime.strftime(f["date"], "%Y.%m.%d") + ".html", "w") as f:
            f.write(report)
        logging.info("Report has been successfully constructed.")
    else:
        logging.info("Report has been constructed before. Check ./reports folder.")
        sys.exit(0)


if __name__ == "__main__":

    # patterns for log file
    NAME = "(?<=nginx-access-ui\.log-)\d+"
    EXT = "(?<=\d{8}).*"


    # dealing with configs
    config_dict = {
        "report_size": 1000,
        "report_dir": "./reports",
        "log_dir": "./server_logs",
        "logging": "./logs/log_analizer.log",
        "error_limit": 0.5,
        "template": "./config/report.html"
    }

    parser = ArgumentParser()
    parser.add_argument("-config", "--config", dest="config", help="Open specified file")
    args = parser.parse_args()
    conf = args.config
    if conf:
        config = configparser.ConfigParser()
        config.read(conf)
    else:
        config = config_dict
    

    # construction of correct config dict
    if len(config) != 0:
        for k in config.keys():
            if k in config_dict.keys():
                config_dict[k] = config[k]
            else:
                config_dict[k] = config[k]
        config = config_dict
    else:
        raise FileNotFoundError


    # logging depending on config
    if "logging" in config.keys():
        logging.basicConfig(format="[%(asctime)s] %(levelname).1s %(message)s:%(lineno)d",
                            level=logging.DEBUG,
                            # filename=config["logging"])
                            filename="./logs/log_analizer.log")
    else:
        logging.basicConfig(format="[%(asctime)s] %(levelname).1s %(message)s:%(lineno)d",
                            level=logging.DEBUG,
                            filename=None)


    # main actions
    try:

        main(logpath=config["log_dir"], 
             reportpath=config["report_dir"], 
             error_limit=config["error_limit"], 
             report_size = int(config["report_size"]),
             templatepath=config["template"],
             name_p=NAME, 
             ext_p=EXT)

    except KeyboardInterrupt as k:
         logging.exception(k)
         raise
    except Exception as e:
         logging.error("Some unforseen error. %s" % e, exc_info=True)