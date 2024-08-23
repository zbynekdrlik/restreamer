document.addEventListener('DOMContentLoaded',  function(){
    var notyf = new Notyf();

    var messages = JSON.parse(document.getElementById('notyf-messages').textContent);
    messages.forEach(function(message) {
        if (message.tags === 'success') {
            notyf.success(message.message);

        } else if (message.tags === 'error') {
            notyf.error(message.message)
        }
    });
});