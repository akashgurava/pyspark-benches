"""
A simple commandline script to measure performance for multiple spark-submit scenarios

Usage:
Uncomment functions you want to test and run spark-submit <script_name.py>

Inferences:
    1. 2 sec overhead coming from spark-submit
    2. spark-submit will not create session in the background. It waits until
        the first getOrCreate call is made.
    3. 2.2 sec for first getOrCreate next getOrCreate are instant
    4. Stopping existing session and creating new takes surprisingly low ~ 0.6 sec


1. When get_sparksession is not called program exits immediately.
The overhead comes from spark-submit to get JVM up and running.
LOCAL:
    Total shell: 2s.
EMR:
    Total shell: 2s.


2. single getOrCreate call will wait till spark session is acquired.
LOCAL:
    Total shell: 4s.
    Print:
        First session took: 2.22. From Program Start: 2.22
LOCAL 1gb:
    Total shell: 11s.
    Print:
        First session took: 5.28. From program start: 5.28
LOCAL 2gb:
    Total shell: 11s.
    Print:
        First session took: 5.32. From program start: 5.32
LOCAL 3gb:
    Total shell: 11s.
    Print:
        First session took: 5.44. From program start: 5.44
LOCAL 4gb:
    Total shell: 12s.
    Print:
        First session took: 6.24. From program start: 6.24
EMR:
    Total shell: 15s.
    Print:
        First session took: 12.86. From program start: 12.86

3. spark-submit will not create session in the background. It waits until
the first getOrCreate call is made
LOCAL:
    Total shell: 4s.
    Print:
        First session took: 2.22. From Program Start: 2.22
EMR:
    Total shell: 18s.
    Print:
        Getting session took: 12.66. From program start: 14.66

4. When calling getOrCreate twice, first call will wait till spark session
completes "Creating". Second will return instantly as it just gets previous
"Created" session.
LOCAL:
    Total shell: 5s.
    Print:
        First session took: 2.18. From Program Start: 2.18
        Second session took: 0.0. From Program Start: 2.18
LOCAL 4gb:
    Total shell: 14s.
    Print:
        First session took: 5.56. From program start: 5.56
        Second session took: 0.0. From program start: 5.56
EMR:
    Total shell: 16s.
    Print:
        First session took: 13.62. From program start: 13.62
        Second session took: 0.0. From program start: 13.62

4. When we call getOrCreate after stopping existing. A new session will get
created.
LOCAL:
    Total shell: 5s.
    Print:
        Initial getOrCreate took: 2.2. From Program Start: 2.2
        Stopping session took: 0.31. From Program Start: 2.52
        Second getOrCreate after stop took: 0.19. From Program Start: 2.71
        Create session took: 2.71. From Program Start: 2.71
LOCAL 4gb:
    Total shell: 12s.
    Print:
        Initial getOrCreate took: 5.49. From program start: 5.49
        Stopping session took: 0.4. From program start: 5.96
        Second getOrCreate after stop took: 0.46. From program start: 6.43
        Create session took: 6.43. From program start: 6.43
EMR:
    Total shell: 23s
    Print:
        Initial getOrCreate took: 12.78. From program start: 12.78
        Stopping session took: 0.88. From program start: 13.69
        Second getOrCreate after stop took: 6.9. From program start: 20.59
        Create session took: 20.59. From program start: 20.59


5. When we call getOrCreate after stopping default. A new session will get
created.
LOCAL:
    Total shell: 6s.
    Print:
        Initial getOrCreate took: 2.14. From Program Start: 2.14
        Stopping session took: 0.28. From Program Start: 2.44
        Second getOrCreate after stop took: 0.21. From Program Start: 2.65
        Create first session took: 2.65. From Program Start: 2.65
        Initial getOrCreate took: 0.02. From Program Start: 2.67
        Stopping session took: 0.4. From Program Start: 3.07
        Second getOrCreate after stop took: 0.19. From Program Start: 3.26
        Create second session took: 0.61. From Program Start: 3.26
LOCAL 4gb:
    Total shell: 14s.
    Print:
        Initial getOrCreate took: 5.46. From program start: 5.46
        Stopping session took: 0.51. From program start: 6.03
        Second getOrCreate after stop took: 0.44. From program start: 6.47
        Create first session took: 6.47. From program start: 6.47
        Initial getOrCreate took: 0.05. From program start: 6.52
        Stopping session took: 0.25. From program start: 6.87
        Second getOrCreate after stop took: 0.46. From program start: 7.33
        Create second session took: 0.86. From program start: 7.33
EMR:
    Total shell: 30s.
    Print:
        Initial getOrCreate took: 11.5. From program start: 11.5
        Stopping session took: 0.37. From program start: 11.91
        Second getOrCreate after stop took: 8.02. From program start: 19.93
        Create first session took: 19.93. From program start: 19.93
        Initial getOrCreate took: 0.03. From program start: 19.95
        Stopping session took: 0.37. From program start: 20.36
        Second getOrCreate after stop took: 7.59. From program start: 27.95
        Create second session took: 8.02. From program start: 27.95

"""
from time import time, sleep


from pyspark.sql import SparkSession  # pylint: disable=wrong-import-position

# from findspark import init

# init()


P_START = time()


def print_time_taken(func, name, *args, **kwargs):
    """
    Prints the time taken to run a function.

    Args:
        func: Function to run

    Returns:
        None or SparkSession: session created if any
    """
    start = time()
    spark = func(*args, **kwargs)
    end = time()
    f_time = round(end - start, 2)
    p_time = round(end - P_START, 2)
    print_stmt = "{} took: {}. From program start: {}"
    print(print_stmt.format(name, f_time, p_time))
    return spark


def simple_sleep(wait):
    """
    Sleep for `wait` seconds.

    Args:
        wait (float): wait time in seconds
    """
    print_time_taken(sleep, "Sleeping for {} sec".format(wait), wait)


def get_sparksession(app_name=None, conf=None):
    """
    Gets a SparkSession. Usually the one created by `spark-submit`.

    Returns:
        SparkSession: session created
    """
    app_name = app_name or "TestGetOrCreate"
    if conf:
        return SparkSession.builder.appName(app_name).config(conf=conf).getOrCreate()
    return SparkSession.builder.appName(app_name).getOrCreate()


def create_sparksession(app_name=None):
    """
    Gets the initial created spark session. Stop it then create a new session.

    Returns:
        SparkSession: session created
    """
    spark = print_time_taken(get_sparksession, "Initial getOrCreate", app_name=app_name)
    conf = spark.sparkContext.getConf()
    print_time_taken(spark.stop, "Stopping session")
    return print_time_taken(
        get_sparksession, "Second getOrCreate after stop", app_name=app_name, conf=conf
    )


def single_get_sparksession():
    """
    Test time taken for getting a spark session.
    """
    print_time_taken(get_sparksession, "First session")


def single_get_sparksession_after_wait(wait):
    """
    Test time taken for Wait for `wait` then getting a spark session.
    """
    simple_sleep(wait)
    print_time_taken(get_sparksession, "Getting session")


def double_get_sparksession():
    """
    Test time taken for getting 2 spark session one after another immediately.
    Prints output for each session creation time.
    """
    print_time_taken(get_sparksession, "First session")
    print_time_taken(get_sparksession, "Second session", "NewTestGetOrCreate")


def single_create_sparksession():
    """
    Test time taken for creating spark session.
    """
    print_time_taken(create_sparksession, "Create session")


def double_create_sparksession():
    """
    Test time taken for calling `create_sparksession`.
    """
    print_time_taken(create_sparksession, "Create first session")
    print_time_taken(create_sparksession, "Create second session")


if __name__ == "__main__":
    # single_get_sparksession()
    # single_get_sparksession_after_wait(2)
    # double_get_sparksession()
    # single_create_sparksession()
    double_create_sparksession()
    # pass
