from io import BytesIO
from base64 import b64encode
from pandas import DataFrame
import matplotlib.pyplot as plot

def create_plot_data(data_frame:DataFrame, title:str, xlabel:str='Date', ylabel:str='USD value'):
    plot.figure(figsize=(14, 8))
    for column in data_frame.columns:
        plot.plot(data_frame.index, data_frame[column], label=column)

    plot.title(title)
    plot.xlabel(xlabel)
    plot.ylabel(ylabel)
    plot.legend(loc='best')
    plot.grid(True)

def write_plot_to_buffer() -> BytesIO:
    plot_buffer = BytesIO()
    plot.savefig(plot_buffer, format='png')
    plot_buffer.seek(0)

    return plot_buffer

def encode_image_to_base64(buffer: BytesIO) -> str:
    buffer.seek(0)
    return b64encode(buffer.read()).decode('utf-8')
