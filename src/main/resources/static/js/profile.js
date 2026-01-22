if (!requireAuth()) {
    throw new Error('Not authenticated');
}

updateWelcomeMessage();

function populateProfile() {
    const token = getToken();
    const decoded = decodeJWT(token) || {};
    const displayName = getDisplayName() || 'User';
    const email = decoded?.sub || 'Unknown';

    document.getElementById('profileName').textContent = displayName;
    document.getElementById('profileEmail').textContent = email;
    document.getElementById('profileDisplayName').textContent = displayName;
    document.getElementById('profileDisplayEmail').textContent = email;
}

populateProfile();

function logout() {
    clearToken();
    window.location.href = '/';
}

document.getElementById('logoutBtn').addEventListener('click', logout);

const deleteAccountForm = document.getElementById('deleteAccountForm');
const deletePasswordInput = document.getElementById('deletePassword');
const deleteAccountMessage = document.getElementById('deleteAccountMessage');
const showDeleteAccountButton = document.getElementById('showDeleteAccount');
const deleteAccountSection = document.getElementById('deleteAccountSection');

function setDeleteAccountMessage(message, isError = false) {
    if (!deleteAccountMessage) return;
    deleteAccountMessage.textContent = message;
    deleteAccountMessage.classList.toggle('error', isError);
}

async function handleAccountDeletion(event) {
    event.preventDefault();
    setDeleteAccountMessage('');

    const password = deletePasswordInput?.value?.trim();
    if (!password) {
        setDeleteAccountMessage('Password is required', true);
        return;
    }

    try {
        await api.delete('/api/user/delete', { password });
        clearToken();
        window.location.href = '/';
    } catch (error) {
        setDeleteAccountMessage(error.message || 'Failed to delete account', true);
    }
}

deleteAccountForm?.addEventListener('submit', handleAccountDeletion);

function revealDeleteAccountForm(event) {
    event?.preventDefault();
    deleteAccountSection?.classList.remove('hidden');
    showDeleteAccountButton?.classList.add('hidden');
    setDeleteAccountMessage('');
    deletePasswordInput?.focus();
}

showDeleteAccountButton?.addEventListener('click', revealDeleteAccountForm);
