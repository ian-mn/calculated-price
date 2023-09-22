from datetime import timedelta
from time import time

import pandas as pd
import win32com.client


def time_decorator(original_func):
    def wrapper(*args, **kwargs):
        start = time()
        result = original_func(*args, **kwargs)
        print("Timer: {}".format(str(timedelta(seconds=int(time() - start)))))
        return result

    return wrapper


@time_decorator
def execute_query(
    QueryText, add_hierarchy=False, server="olap2-arka", cube="Analyse United"
):
    Conn = win32com.client.Dispatch("ADODB.Connection")
    Cellset = win32com.client.Dispatch("ADOMD.Cellset")
    Conn.ConnectionString = f"PROVIDER=MSOLAP; persist security info=true; Data Source={server}; initial catalog={cube};"
    Conn.Open
    Cellset.ActiveConnection = Conn
    Cellset.Source = QueryText
    Cellset.Open()
    cols = [
        Cellset.Axes(0).Positions(j).Members(0).Caption
        for j in range(Cellset.Axes(0).Positions.Count)
    ]
    vals = [
        [Cellset(j, i).Value for j in range(Cellset.Axes(0).Positions.Count)]
        for i in range(Cellset.Axes(1).Positions.Count)
    ]

    df = pd.DataFrame(vals, columns=cols).fillna(value=0)

    hierarchy = []
    df_hierarchy = None
    if add_hierarchy:
        len_i = Cellset.Axes(1).Positions.Count
        len_j = Cellset.Axes(1).Positions(1).Members.Count
        for i in range(len_i):
            row = []
            for j in range(len_j):
                row.append(Cellset.Axes(1).Positions(i).Members(j).Caption)
            hierarchy.append(row)
        df_hierarchy = pd.DataFrame(hierarchy)

    Cellset.Close()
    Conn.Close

    if add_hierarchy:
        return pd.concat([df_hierarchy, df], axis=1)
    return df
