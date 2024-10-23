const iconConfig = {
  // iconSize: [64, 64], // size of the icon
  iconSize: [48, 48], // size of the icon
  shadowSize: [50, 64], // size of the shadow
  iconAnchor: [22, 94], // point of the icon which will correspond to marker's location
  shadowAnchor: [4, 62], // the same for the shadow
  popupAnchor: [-3, -76], // point from which the popup should open relative to the iconAnchor
};

const carIcon = L.divIcon({
  iconSize: [28, 28],
  className: "position-relative rotate--marker",
  html: '<div><img style="width: 28px;" src="https://www.pngkit.com/png/full/54-544296_red-top-view-clip-art-at-clker-cartoon.png" /></div>',
});

const greenIcon = L.icon({
  iconUrl: "static/icons/green.png",
  //   shadowUrl: "icon/green.png",
  ...iconConfig,
});

const yellowIcon = L.icon({
  iconUrl: "static/icons/yellow.png",
  //   shadowUrl: "icon/green.png",
  ...iconConfig,
});

const redIcon = L.icon({
  iconUrl: "static/icons/red.png",
  //   shadowUrl: "icon/green.png",
  ...iconConfig,
});

async function readJsonFile() {
  try {
    const res = await fetch("/api/data");
    if (!res.ok) {
      throw new Error(`HTTP error! Status: ${res.status}`);
    }
    const data = await res.json();
    return JSON.parse(data);
  } catch (error) {
    return console.error("Unable to fetch data:", error);
  }
}

async function getCountByTime({ date, time }) {
  try {
    const res = await fetch(`/api/get-count?date=${date}&time=${time}`);
    if (!res.ok) {
      throw new Error(`HTTP error! Status: ${res.status}`);
    }
    const data = await res.json();
    return JSON.parse(data);
  } catch (error) {
    return console.error("Unable to fetch data:", error);
  }
}

const markers = {};

const moveableMarkers = L.moveMarker(
  [[10.8279878, 106.719228]],
  {
    animate: true,
    color: "red",
    weight: 4,
    hidePolylines: false,
    duration: 5000,
    removeFirstLines: true,
    maxLengthLines: 3,
  },
  {
    animate: true,
    hideMarker: false,
    duration: 5000,
    speed: 0,
    followMarker: false,
    rotateMarker: true,
    rotateAngle: 210,
    icon: L.divIcon({
      iconSize: [28, 28],
      className: "position-relative rotate--marker",
      html: '<div><img style="width: 28px;" src="https://www.pngkit.com/png/full/54-544296_red-top-view-clip-art-at-clker-cartoon.png" /></div>',
    }),
  },
  {}
);

async function initMap() {
  const map = L.map("map");

  L.tileLayer("http://{s}.tile.osm.org/{z}/{x}/{y}.png", {
    attribution:
      '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors',
  }).addTo(map);

  // init routing control
  const control = L.Routing.control(
    L.extend(window.lrmConfig, {
      waypoints: [
        // L.latLng(10.8279878, 106.719228),
        // L.latLng(10.7610585, 106.6657831),
      ],

      autoRoute: false,
      lineOptions: {
        styles: [{ className: "animate" }],
      },
      createMarker: function (i, waypoint, n) {
        return L.marker(
          waypoint.latLng,
          i === 0
            ? { icon: carIcon, draggable: true, rotationAngle: 60 }
            : { draggable: true, bounceOnAdd: false }
        )
          .bindPopup(waypoint.latLng)
          .openPopup();
      },
      geocoder: L.Control.Geocoder.nominatim(),
      router: L.Routing.osrmv1({
        serviceUrl: "https://router.project-osrm.org/route/v1",
        alternatives: true, // Enable alternatives
      }),
      showAlternatives: true,
      altLineOptions: {
        styles: [
          { color: "black", opacity: 0.15, weight: 9 },
          { color: "white", opacity: 0.8, weight: 6 },
          { color: "blue", opacity: 0.5, weight: 2 },
        ],
      },
      routeWhileDragging: true,
      //reverseWaypoints: true,
    })
  ).addTo(map);
  // init camera
  const cameras = await readJsonFile();
  const groupCameras = cameras.map((camera) => {
    const { latitude, longitude, node_name, node_id } = camera;
    const marker = L.marker([latitude, longitude], { icon: greenIcon })
      .bindPopup(`${node_name}-Xe:0-Vắng vẻ`)
      .openPopup();
    markers[node_id] = marker;
    return marker;
  });
  L.layerGroup(groupCameras).addTo(map);

  // get routes
  const onRouteSelected = new Promise((resolve, reject) => {
    control.on("routeselected", function (e) {
      const route = e.route;
      const alternatives = e.alternatives;
      resolve({
        coordinates: route.coordinates,
        waypoints: route.waypoints,
        alternatives,
      });
    });
  });

  // find routes
  document.getElementById("find").addEventListener("click", () => {
    control.route();
  });
  // trigger start simulator
  document.getElementById("start").addEventListener("click", async () => {
    const { coordinates, waypoints, alternatives } = await onRouteSelected;
    const end = waypoints[1];

    let index = 0;

    setInterval(function () {
      if (index === 30) {
        const lis = document.getElementById("notificationList").childNodes;
        console.log("lis", lis);
        control.route();
        index = 0;

      }
      
      if (coordinates.length > 0) {
        const { lat, lng } = coordinates.shift();
        // control.spliceWaypoints(0, 1);
        control.setWaypoints([
          L.latLng(lat, lng),
          L.latLng(end.latLng.lat, end.latLng.lng),
        ]);
        index++;
      }
    }, 500);
  });

  // show error
  L.Routing.errorControl(control).addTo(map);
}

initMap();
