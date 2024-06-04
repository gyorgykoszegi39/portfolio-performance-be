'''
ploting.py

This module provides basic operation to visualize the dataset.

Functions:
    create_plot_data(data_frame, title, xlabel, ylabel): Creates and sets up a plot figure.
    write_plot_to_buffer(): Returns the buffer of the plot figure.
    encode_image_to_base64(buffer): Returns the utf-8 encoding of the buffer.
'''

from io import BytesIO
from base64 import b64encode
from pandas import DataFrame
import matplotlib.pyplot as plot

def create_plot_data(data_frame:DataFrame, title:str, xlabel:str='Date', ylabel:str='USD value'):
    '''
    Creates diagrams of the dataset.

    Args:
        data_frame:
            The data to be plotted.
        title:
            The title of the diagram.
        xLabel:
            The measure on the x axis. By default is 'Date'.
        yLabel:
            The measure on the y axis. By default is 'USD value'.
    '''
    plot.figure(figsize=(14, 8))
    for column in data_frame.columns:
        plot.plot(data_frame.index, data_frame[column], label=column)

    plot.title(title)
    plot.xlabel(xlabel)
    plot.ylabel(ylabel)
    plot.legend(loc='best')
    plot.grid(True)

def write_plot_to_buffer() -> BytesIO:
    '''Creates and returns a byte buffer from the diagram in the plot.'''

    plot_buffer = BytesIO()
    plot.savefig(plot_buffer, format='png')
    plot_buffer.seek(0)

    return plot_buffer

def encode_image_to_base64(buffer: BytesIO) -> str:
    '''Encodes bytes to string value.'''

    buffer.seek(0)
    return b64encode(buffer.read()).decode('utf-8')
