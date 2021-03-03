import pickle
import numpy as np
import matplotlib.pyplot as plt


def PlotDistribution(x, y, y_scale='log', labels=("x", "y")):
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    ax.bar(x, y)
    ax.set_yscale(y_scale)
    ax.set_ylabel(labels[1])
    ax.set_xlabel(labels[0])
    fig.canvas.draw()


def PrepareLabels(points):
    """ Create x, y

    Args:
        points (array of arrays, lengthx2): Data points, each index is a datapoint with x and y.

    Returns:
        (x: array, y: array): tuple of two arrays. Joins based on indices.
    """
    max_label = points.transpose()[0].max()
    x = np.arange(max_label + 1)
    y = np.zeros(max_label + 1)
    for point in points:
        y[point[0]] = point[1]
    return x, y


def ToPercentages(y):
    total = y.sum()
    y = y / total
    return y


if __name__ == "__main__":
    print("Start plotting distribution.")

    label_distribution = pickle.load(
        open("analysis/local/labelDistribution.pickle", "rb"), encoding='latin1')
    label_distribution_points = np.array(label_distribution['data_points'])
    x, y = PrepareLabels(label_distribution_points)
    PlotDistribution(x, y, y_scale='linear', labels=("changeCount", "occurences"))
    PlotDistribution(x, ToPercentages(y), y_scale='linear', labels=("changeCount", "occurenceRatio"))
    plt.show()

    print("End plotting.")
