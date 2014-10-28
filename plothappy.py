"""Collection of functions for plotting
"""

from matplotlib import pyplot
import os.path

def prep_filepath(filename, savedir, ext='.png'):
    if not filename.endswith(ext):
        filename += ext
    filepath = os.path.join(savedir, filename)
    return filepath

def make_hist(data, filename,
              title=None, xlabel=None, ylabel='N', nbins=20, 
              **kwargs):
    """Make a histogram"""
    if title is None:
        title = filename
    if xlabel is None:
        xlabel = filename
    fig = pyplot.figure()
    ax = fig.gca()
    n, bins, patches = ax.hist(data, nbins, **kwargs)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(True)
    return fig

def show_hist(data, filename, legend=False, **kwargs):
    """Show a histogram"""
    fig = make_hist(data, filename, **kwargs)
    if legend is not False:
        pyplot.legend()
    pyplot.show()
    pyplot.close(fig)
    return "<screen>"

def save_hist(data, filename, savedir='.', savefig={}, **kwargs):
    """Save a histogram"""
    fig = make_hist(data, filename, **kwargs)
    filepath = prep_filepath(filename, savedir)
    return save(fig, filename, savedir, savefig, **kwargs)

def make_plot(x, y, filename,
              xerr=None, yerr=None,
              fig=None,
              xlog=False, ylog=False,
              xlim=None, ylim=None,
              title=None, xlabel=None, ylabel=None,
              fontsize='medium',
              color='black', lw=0, marker='o', ms=3,
              figure={},
              margins=None,
              minorticks=False,
              tick_params=None,
              xgrid=False, ygrid=False,
              subplots_adjust=None,
              tight_layout={},
              **kwargs
              ):
    """Make a plot"""
    # Note: Order or execution matters on some of these settings.

    # Figure titles and labels
    if fig is None:
        fig = pyplot.figure(**figure)
        ax = fig.gca()
        if title is None:
            title = filename
        if ylabel is None:
            ylabel = filename
        ax.set_title(title, fontsize=fontsize)
        ax.set_ylabel(ylabel, fontsize=fontsize)
    else:
        ax = fig.gca()

    # Figure scale
    if xlog:
        ax.set_xscale('log')
    if ylog:
        ax.set_yscale('log')

    # Figure margins
    if margins:
        pyplot.margins(*margins)
        
    # Figure ticks and grids
    if minorticks:
        ax.minorticks_on()
    if tick_params:
        if isinstance(tick_params, dict):
            tick_params = [ tick_params ]
        for tp in tick_params:
            ax.tick_params(**tp)
    if xgrid:
        xgrid['b'] = True
        ax.xaxis.grid(**xgrid)
    if ygrid:
        ygrid['b'] = True
        ax.yaxis.grid(**ygrid)

    if subplots_adjust:
        pyplot.subplots_adjust(**subplots_adjust)
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=fontsize)

    # Plot
    pyplot.plot(x, y, color=color, lw=lw, marker=marker, ms=ms, **kwargs)

    # Plot limits
    if xlim:
        ax.set_xlim(*xlim)
    if ylim:
        ax.set_ylim(*ylim)

    # Error bars
    if xerr is not None:
        pyplot.errorbar(x, y, xerr=xerr, ecolor=color, fmt=None)
    if yerr is not None:
        pyplot.errorbar(x, y, yerr=yerr, ecolor=color, fmt=None)

    # Layout
    if tight_layout:
        pyplot.tight_layout(**tight_layout)
    return fig

def show_plot(x, y, filename, **kwargs):
    """Show a line plot"""
    fig = make_plot(x, y, filename, **kwargs)
    pyplot.show()
    pyplot.close(fig)
    return "<screen>"

def save_plot(x, y, filename, savedir='.', savefig={}, **kwargs):
    """Save a scatter plot"""
    fig = make_plot(x, y, filename, **kwargs)
    return save(fig, filename, savedir, savefig, **kwargs)
    

def save(fig, filename, savedir='.', savefig={}, **kwargs):
    """Save a figure"""
    filepath = prep_filepath(filename, savedir)
    fig.savefig(filepath, **savefig)
    pyplot.close(fig)
    return filepath
