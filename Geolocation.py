from geopy.geocoders import Nominatim
class Location:

    def __init__(self):
        self.geolocator = Nominatim()
        pass

    def GetLocation(self, latitude, longitude):

        coordinate = str(latitude) + ',' + str(longitude)
        location = self.geolocator.reverse(coordinate)
        return location.address