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