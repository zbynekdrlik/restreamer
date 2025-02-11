document.addEventListener('DOMContentLoaded', function(){
    var notyf = new Notyf();

    // Get the JSON data from the script tag
    var messagesElement = document.getElementById('notyf-messages');
    if (messagesElement && messagesElement.textContent) {
        var messages = JSON.parse(messagesElement.textContent); // Parse the JSON
        messages.forEach(function(message) {
            if (message.tags === 'success') {
                notyf.success(message.message);
            } else if (message.tags === 'error') {
                notyf.error(message.message);
            }
        });
    }
});

function showBootstrapToast(message, type) {
    // Create (or get) a container for the toasts.
    let container = document.getElementById('toast-container');
    if (!container) {
      container = document.createElement('div');
      container.id = 'toast-container';
      // Position container at the top-right corner.
      container.className = 'toast-container position-fixed top-0 end-0 p-3';
      document.body.appendChild(container);
    }

    // Create the outer toast element.
    const toast = document.createElement('div');
    toast.className = 'toast';
    toast.setAttribute('role', 'alert');
    toast.setAttribute('aria-live', 'assertive');
    toast.setAttribute('aria-atomic', 'true');

    // (Optional) Create a toast header.
    const toastHeader = document.createElement('div');
    toastHeader.className = 'toast-header';

    const strong = document.createElement('strong');
    strong.className = 'me-auto';
    strong.textContent = type === 'error' ? 'Error' : 'Notification';
    toastHeader.appendChild(strong);

    // Optional close button in the header.
    const closeButton = document.createElement('button');
    closeButton.type = 'button';
    closeButton.className = 'btn-close';
    closeButton.setAttribute('data-bs-dismiss', 'toast');
    closeButton.setAttribute('aria-label', 'Close');
    toastHeader.appendChild(closeButton);

    // Add the header to the toast.
    toast.appendChild(toastHeader);

    // Create the toast body.
    const toastBody = document.createElement('div');
    toastBody.className = 'toast-body';
    toastBody.textContent = message;
    toast.appendChild(toastBody);

    // Append the toast to the container.
    container.appendChild(toast);

    // Initialize and show the toast with a delay of 3000ms (3 seconds).
    const bsToast = new bootstrap.Toast(toast, { delay: 3000 });
    bsToast.show();

    // Remove the toast from the DOM once it has been hidden.
    toast.addEventListener('hidden.bs.toast', () => {
      toast.remove();
    });
  }