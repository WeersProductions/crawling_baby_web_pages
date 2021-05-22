import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import holoviews as hv
from holoviews import opts, dim
from math import floor
from bokeh.plotting import show
hv.extension('bokeh')


def plot_diff_column(df, dates, column_name, include):
    x = []
    for index, date in enumerate(dates):
        if include[index]:
            x.append(date)


    points = df[column_name].to_numpy()
    print(points.shape, points[0])
    points = np.stack(points)[:30000].transpose()
    print(points.shape, points)
    points = points[include,...]

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    ax.plot(x, points)
    ax.set_ylabel(column_name)
    ax.set_xlabel("dates")
    fig.canvas.draw()


def plot_distributions(full_df, dates):
    dates_str = ['{0}-{1:02d}-{2:02d}'.format(date[0], date[1], date[2]) for date in dates]
    # External
    df = pd.DataFrame(full_df["diffExternalOutLinks"].to_list(), columns=dates_str)
    print(df.head())
    for date_str in dates_str:
        print(df[[date_str]])
        print(df[[date_str]].groupby([date_str]).size())
        # df.hist(bins=range(1,20), column=date_str, log=True, align='left', rwidth=1)
        # plt.show()

    # Internal
    df = pd.DataFrame(full_df["diffInternalOutLinks"].to_list(), columns=dates_str)
    print(df.head())
    for date_str in dates_str:
        # print(df[[date_str]])
        print(df[[date_str]].groupby([date_str]).size())
        # df.hist(bins=range(1,20), column=date_str, log=True, align='left', rwidth=1)
        # plt.show()


def quantize(value, diff_classes):
    for index, bound in enumerate(diff_classes):
        if value <= bound:
            return index
    return len(diff_classes)

    # TODO: instead of hardcoding, allow the amount of classes to be dynamic.
    if value == 0:
        return 0
    if value < 10:
        return 1
    if value < 20:
        return 2
    return 3


def create_directions(row, n_different_classes):
    # print(type(row), row)
    result = []
    last_value = None
    for value in row:
        # print(value)
        if last_value == None:
            last_value = value
            continue

        new_class = last_value * n_different_classes + value
        result.append(new_class)
    return result


def plot_sankey(full_df, dates, column_name, diff_classes=[0,10,20]):
    n_diff_classes = len(diff_classes) + 1
    dates_str = ['{0}-{1:02d}-{2:02d}'.format(date[0], date[1], date[2]) for date in dates]

    # Expand df
    df = pd.DataFrame(full_df[column_name].to_list(), columns=dates_str)

    # Create the node dictionary.
    for date_str in dates_str:
        df[date_str] = df[date_str].apply(quantize, args=(diff_classes,))

    print(df)
    df = df.apply(create_directions, axis=1, result_type='expand', args=(n_diff_classes,))
    print(df)

    # nodes = ["1: 0", "1: 1-10", "1: 11-20", "1: >21", "2: 0", "2: 1-10", "2: 11-20", "2: "]
    nodes = []
    for index in range(len(dates)):
        last_value = None
        for bound in diff_classes:
            name_str = None
            if last_value == None:
                name_str = bound
            else:
                name_str = f"{last_value + 1}-{bound}"
            last_value = bound
            nodes.append(f"{index}: {name_str}")
        else:
            nodes.append(f"{index}: >{last_value + 1}")
    print(nodes)
    nodes = hv.Dataset(enumerate(nodes), 'index', 'label')
    edges = []
    for column_name in range(len(dates) - 1):
        # Count occurences.
        group_count = df[column_name].value_counts()
        group_sum = group_count.sum()
        print(group_count, group_sum)
        for index, occurence in group_count.iteritems():
            print(index, occurence)
            from_node = floor(index / n_diff_classes) + column_name * n_diff_classes
            to_node = index % n_diff_classes + (column_name + 1) * n_diff_classes
            edges.append((from_node, to_node, occurence / group_sum * 100))

    print(edges)
    value_dim = hv.Dimension('Percentage', unit='%')
    flow = hv.Sankey((edges, nodes), ['From', 'To'], vdims=value_dim)
    something = flow.opts(
        opts.Sankey(labels='label', label_position='right', width=1800, height=1200, cmap='Set1', edge_color=dim('To').str(), node_color=dim('index').str()))
    something2 = hv.render(something)
    show(something2)


def plot_sankeys(full_df, dates):
    plot_sankey(full_df, dates, "diffExternalOutLinks")
    plot_sankey(full_df, dates, "diffInternalOutLinks")


def diff_line_graph(full_df, dates, include):
    x = ['{0}-{1:02d}-{2:02d}'.format(date[0], date[1], date[2]) for date in dates]
    # plot_diff_column(full_df, x, "diffExternalOutLinks", include)
    plot_diff_column(full_df, x, "diffInternalOutLinks", include)
    plt.show()


def main(parquet_file, dates, include):
    print("Starting")
    df = pd.read_parquet(parquet_file)

    plot_sankeys(df, dates)
    return
    plot_distributions(df, dates)
    diff_line_graph(df, dates, include)


if __name__ == "__main__":
    parquet_file = "analysis/local/data/web_timeseries.parquet"
    dates = [(2020, 7, 13),
        (2020, 7, 20),
        (2020, 7, 28),
        (2020, 8, 6),
        (2020, 8, 11),
        (2020, 8, 17),
        (2020, 8, 27),
        (2020, 9, 1),
        (2020, 9, 7),
        (2020, 9, 14)][1:]

    include = np.ones(len(dates), dtype=bool)
    include[[2, 4]] = False
    

    main(parquet_file, dates, include)
