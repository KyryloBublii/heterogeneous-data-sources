if (!requireAuth()) {
    throw new Error('Not authenticated');
}

updateWelcomeMessage();

document.getElementById('logoutBtn').addEventListener('click', () => {
    clearToken();
    window.location.href = '/';
});
