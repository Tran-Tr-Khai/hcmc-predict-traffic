// script.js

function addNotification(message) {
  const notificationList = document.getElementById("notificationList");
  
  // Create a new list item
  const newNotification = document.createElement("li");
  newNotification.textContent = message;

  // Append the new notification to the list
  notificationList.appendChild(newNotification);

  // Show the snackbar
  showSnackbar();

  // Automatically hide the snackbar after 3 seconds
  //setTimeout(hideSnackbar, 3000);
}

function showSnackbar() {
  const snackbar = document.getElementById("snackbar");
  snackbar.className = "show";
}

function hideSnackbar() {
  const snackbar = document.getElementById("snackbar");
  snackbar.className = snackbar.className.replace("show", "");
}

