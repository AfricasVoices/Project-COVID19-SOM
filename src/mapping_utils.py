import matplotlib.pyplot as plt
import pandas
from matplotlib.colors import LinearSegmentedColormap


class MappingUtils(object):
    AVF_COLOR_MAP = LinearSegmentedColormap.from_list("avf_color_map", ["#e6cfd1", "#993e46"])

    @classmethod
    def plot_frequency_map(cls, geo_data, admin_id_column, frequencies):
        """
        Plots a map of the given geo data with a choropleth showing the frequency of responses in each administrative
        region.

        The map is plotted to the active matplotlib figure. Use matplotlib.pyplot to access and manipulate the result.

        :param geo_data: GeoData to plot.
        :type geo_data: geopandas.GeoDataFrame
        :param admin_id_column: Column in `geo_data` of the administrative region ids.
        :type admin_id_column: str
        :param frequencies: Dictionary of admin_id -> frequency.
        :type frequencies: dict of str -> int
        """
        # Convert the raw frequencies dict to a pandas DataFrame, then join it with the geo data frame on the admin_ids.
        # Frequencies that are 0 are set to None ('missing'), to prevent the color-mapping algorithm including
        # low responses (e.g. just 1 or 2) in the same class (color) as regions with no response.
        map_frequencies = []
        for k, v in frequencies.items():
            map_frequencies.append({
                admin_id_column: k,
                "Frequency": v if v != 0 else None
            })
        geo_data = geo_data.merge(pandas.DataFrame(map_frequencies), on=admin_id_column)

        # Plot the choropleth map.
        # Frequencies are classed using the Fisher-Jenks method, a standard GIS algorithm for choropleth classification.
        # Using this method prevents a region with a vastly higher frequency than the others (e.g. a capital city)
        # from using up all of the colour range, as would happen with a linear scale.
        unique_frequencies = {f for f in frequencies.values() if f != 0}
        number_of_classes = min(5, len(unique_frequencies))
        if number_of_classes > 0:
            geo_data.plot(column="Frequency", cmap=cls.AVF_COLOR_MAP,
                          scheme="fisher_jenks", k=number_of_classes,
                          linewidth=0.1, edgecolor="black",
                          missing_kwds={"edgecolor": "black", "facecolor": "white"})
        else:
            # Special-case plotter for when all frequencies are 0, because geopandas crashes otherwise.
            geo_data.plot(linewidth=0.1, edgecolor="black", facecolor="white")
        plt.axis("off")

        # Add a label to each administrative region showing its absolute frequency.
        # The font size and label position names are currently hard-coded for Kenyan counties.
        # TODO: Modify once per-map configuration needs are better understood by testing on other maps.
        for i, admin_region in geo_data.iterrows():
            if pandas.isna(admin_region.ADM1_CALLX):
                xy = (admin_region.ADM1_LX, admin_region.ADM1_LY)
                xytext = None
            else:
                xy = (admin_region.ADM1_CALLX, admin_region.ADM1_CALLY)
                xytext = (admin_region.ADM1_LX, admin_region.ADM1_LY)

            plt.annotate(s=frequencies[admin_region[admin_id_column]],
                         xy=xy, xytext=xytext,
                         arrowprops=dict(facecolor="black", arrowstyle="-", linewidth=0.1, shrinkA=0, shrinkB=0),
                         ha="center", va="center", fontsize=3.8)
