import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def plot_bar_x(data, labels, xlabel, ylabel, title):
    plt.bar(labels, data)
    plt.ylabel(ylabel, fontsize=10)
    plt.xlabel(xlabel, fontsize=10)
    # plt.xticks(data, labels, fontsize=5, rotation=30)
    plt.title(title)
    plt.show()

def plot_grouped_bars(task1_res, task2_res, task3_res, labels, x_label):
    barWidth = 0.25

    # labels = ['Accuracy', 'Precision', 'Recall', 'F1']

    plt.figure(figsize=(15, 10))

    x = np.arange(len(labels))
    width = 0.25

    r1 = np.arange(len(labels))
    r2 = [x + barWidth for x in r1]
    r3 = [x + barWidth for x in r2]

    fig, ax = plt.subplots()
    rects1 = ax.bar(r1, task1_res, width, label='task1', color='#B2FF00')
    rects2 = ax.bar(r2, task2_res, width, label='task2', color='#FFC300')
    rects3 = ax.bar(r3, task3_res, width, label='task3', color='#FF5733')

    ax.set_ylabel('Mean execution time in sec.')
    ax.set_xlabel(x_label)
    ax.set_title('Comparison')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend(loc='best')

    def autolabel(rects):
        for rect in rects:
            height = rect.get_height()
            ax.annotate('{}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, -1),
                        textcoords="offset points",
                        size='8',
                        ha='center', va='bottom')

    autolabel(rects1)
    autolabel(rects2)
    autolabel(rects3)

    plt.xticks([r + width for r in range(len(rects1))], labels)
    plt.show()


def add_skyline_scores_to_task3():
    skyline_times = df1['time'].tolist()
    print(skyline_times)

    for i in range(0, len(df3)):
        # df3.loc[i]['time'] = 10
        df3.loc[i, 'time'] = int(df3.iloc[i]['time']) + skyline_times[i]

    print(df3)
    df3.to_excel('task3-results.xlsx', index=False)


def get_times_by_distribution(df):
    anticorrelated = df[:4].tolist()
    correlated = df[4:8].tolist()
    normal = df[8:12].tolist()
    uniform = df[12:16].tolist()
    return anticorrelated, correlated, normal, uniform


def plot_lines(x, anti, corr, norm, uni, x_label, y_label, title):
    plt.plot(x, anti, label='anticorrelated',  marker='o')
    plt.plot(x, corr, label='correlated', marker='o')
    plt.plot(x, norm, label='normal', marker='o')
    plt.plot(x, uni, label='uniform', marker='o')
    plt.legend(loc='best')
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.show()


def groups(df, task):
    print("---------------------")
    print(task)
    print("---------------------")

    distributions = df.groupby(['distribution'])['time'].mean()
    dimensions = df.groupby(['dim'])['time'].mean()
    n_samples = df.groupby(['n_samples'])['time'].mean()

    samples_distribution = df.groupby(['distribution', 'n_samples'])['time'].mean()
    anti, corr, norm, uni = get_times_by_distribution(samples_distribution)
    x = ['100', '1000', '10000', '100000']
    plot_lines(x, anti, corr, norm, uni, 'n_samples', 'time', task)

    dim_distribution = df.groupby(['distribution', 'dim'])['time'].mean()
    anti, corr, norm, uni = get_times_by_distribution(dim_distribution)
    x = ['2', '3', '4', '5']
    plot_lines(x, anti, corr, norm, uni, 'dimensions', 'time', task)

    return distributions, dimensions, n_samples


def create_bar_plot(df, task, x_label, y_label):

    df.plot(kind="bar")
    plt.xticks(rotation=30, horizontalalignment="center")
    plt.title("Task " + str(task) + " mean time")
    plt.xlabel(x_label)
    plt.ylabel(y_label)

    plt.show()


def list_bar_plot(data, title, x_label, y_label):
    fig, ax = plt.subplots()

    labels = ['anticorrelated', 'correlated', 'normal', 'uniform']

    data_100 = [100, 100, 100, 100]
    # creating the bar plot
    plt.bar(labels, data_100, color='lightblue', width=0.4)
    bar_plot = plt.bar(labels, data, color='orange', width=0.4)

    def autolabel(rects):
        for idx, rect in enumerate(bar_plot):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2., 0.4*height,
                    str(int(data[idx])) + '%',
                    ha='center', rotation=0)

    autolabel(bar_plot)

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)

    # plt.xticks(fontsize=14)
    # for spine in plt.gca().spines.values():
    #     spine.set_visible(False)
    # plt.yticks([])



    plt.show()


df1 = pd.read_excel('results/task1-results.xlsx',  engine='openpyxl')
df2 = pd.read_excel('results/task2-results.xlsx',  engine='openpyxl')
df3 = pd.read_excel('results/task3-results.xlsx',  engine='openpyxl')

core1_task2 = pd.read_excel('results/core1_task2.xlsx',  engine='openpyxl')
core_opt_task2 = pd.read_excel('results/opt_cores_task2.xlsx',  engine='openpyxl')
df3_join = pd.read_excel('results/task3-cross-join-results.xlsx',  engine='openpyxl')

# ------------ TASK 1 ---------------
distribution_1, dimension_1, n_samples_1 = groups(df1, 'task1')

# ------------ TASK 2 ----------------

distribution_2, dimension_2, n_samples_2 = groups(df2, 'task2')

# ------------ TASK 3 ----------------
distribution_3, dimension_3, n_samples_3 = groups(df3, 'task3')

# ================ GROUPED BARCHARTS ==========================
labels_1 = ['anticorrelated', 'correlated', 'nromal', 'uniform']
labels_2 = ['2', '3', '4', '5']
labels_3 = ['100', '1000', '10000', '100000']

plot_grouped_bars(distribution_1.tolist(), distribution_2.tolist(), distribution_3.tolist(), labels_1, 'distributions')
plot_grouped_bars(dimension_1.tolist(), dimension_2.tolist(), dimension_3.tolist(), labels_2, 'dimensions')
plot_grouped_bars(n_samples_1.tolist(), n_samples_2.tolist(), n_samples_3.tolist(), labels_3, 'n_samples')

y_label = "execution time in sec."

count_pruned_points = df2.groupby(['distribution'])['# deleted'].sum()
print(count_pruned_points.tolist())
total_points = df2.groupby(['distribution'])['n_samples'].sum()
print(total_points.tolist())

percentages_of_deleted = [(int(a) / int(b))*100 for a,b in zip(count_pruned_points, total_points)]

print(percentages_of_deleted)
# create_bar_plot(count_pruned_points, 'task2', 'distributions', 'percentage of pruned points %')

list_bar_plot(percentages_of_deleted, 'Percentages of deleted points per distribution', 'distributions', '% deleted points')


# ================ COMPARE CORES ======================
print(core1_task2)
core2_task2 = df2.head(len(core1_task2))
core_opt_task2 = core_opt_task2.head(len(core1_task2))
mean_time_c1 = core1_task2['time'].mean()
mean_time_c2 = core2_task2['time'].mean()
mean_time_opt = core_opt_task2['time'].mean()
print(mean_time_c1)
print(mean_time_c2)
print(mean_time_opt)
labels = ['local[1]', 'local[2]', 'local[*]']
times = [mean_time_c1, mean_time_c2, mean_time_opt]
title = 'Mean time for Task2 using different number of cores'
xlabel = '# of cores'
ylabel = 'Average Time in sec.'
plot_bar_x(times, labels, xlabel, ylabel, title)
# =====================================================

# compare 2 different approaches of Task3
print(df3_join)
df3_mp = df3.head(len(df3_join))
mean_time1 = df3_mp['time'].mean()
mean_time2 = df3_join['time'].mean()
print(mean_time1)
print(mean_time2)

labels = ['MapPartition', 'CrossJoin']
times = [mean_time1, mean_time2]
xlabel = ''
ylabel = 'Average Time in sec.'
title = 'Comparison between 2 different approaches for Task3'
plot_bar_x(times, labels, xlabel, ylabel, title)
