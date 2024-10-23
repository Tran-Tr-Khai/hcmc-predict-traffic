const date = new Date("2024-07-23 8:21:00");
const initialDate = date.getTime();
let minutes = 20;
let counter = 0; // Biến đếm để theo dõi số lần hàm được gọi
const apiCallInterval = 5; // Số lần interval trước khi gọi API

document.getElementById("date").innerHTML = date.toLocaleDateString();

const x = setInterval(function () {
  const initialHours = new Date(initialDate).getHours();

  minutes += 1;
  const totalHours = initialHours + Math.floor(minutes / 60);
  const displayMinutes = minutes % 60;

  document.getElementById("time").innerHTML = totalHours + " : " + displayMinutes;

  // Tăng biến đếm
  counter++;

  // Kiểm tra nếu biến đếm đạt đến apiCallInterval
  if (counter >= apiCallInterval) {
    getCountByTime({
      date: "2024-07-23",
      time: `${totalHours}:${displayMinutes}`,
    }).then(function (res) {
      if (!res) return;
      console.log(res);

      // Xóa toàn bộ bảng thông báo trước khi thêm thông báo mới
      const notificationList = document.getElementById("notificationList");
      notificationList.innerHTML = "";

      res.forEach(({ Sensor, Count, Predicted_Count, latitude, longitude }) => {
        const camera = markers[Sensor];
        const status =
          Count < 10 ? "Vắng vẻ" : Count < 30 ? "Bình thường" : "Rất đông";
        if (Count < 10) {
          camera.setIcon(greenIcon);
        } else if (Count < 30) {
          camera.setIcon(yellowIcon);
        } else {
          camera.setIcon(redIcon);
        }
        const [name] = camera.getPopup().getContent().split("-");
        camera.getPopup().setContent(`${name}-Xe: ${Count}-${status}`);

        if (Predicted_Count > 30) {
          addNotification(
            `Camera: ${Sensor} - ${name} - Dự tính: ${Math.ceil(Predicted_Count)} Xe - Vị trí: ${latitude},${longitude}`);
        }
      });
    });

    // Đặt lại biến đếm sau khi gọi API
    counter = 0;
  }
}, 10000); // Đặt lại thời gian interval thành 3 giây.