import unittest
import unittest.mock as mock

import cifutils


class TestCreate(unittest.TestCase):

    def test_empty_credentials_and_domain_for_bad_dconf(self):
        junk_dconf = {"junk1": "123"}
        junk_session = 123
        credentials, domain = cifutils.getCIFCredentials(junk_dconf,
                                                         junk_session,
                                                         prefix="cifs")
        self.assertEqual(credentials, None)
        self.assertEqual(domain, None)

    def test_empty_credentials_and_username(self):
        junk_dconf = {"junk1": "123"}
        junk_session = 123
        credentials, domain = cifutils.getCIFCredentials(junk_dconf,
                                                         junk_session,
                                                         prefix='cifs')
        self.assertEqual(credentials, None)
        self.assertEqual(domain, None)

    def test_password_and_username(self):
        junk_dconf = {"cifspassword": "123", "username": "jsmith"}
        junk_session = 123
        credentials, domain = cifutils.getCIFCredentials(junk_dconf,
                                                         junk_session,
                                                         prefix="cifs")
        expected_credentials = {"USER": "jsmith", "PASSWD": "123"}
        self.assertEqual(credentials, expected_credentials)
        self.assertEqual(domain, None)

    def test_password_and_username_smbsr(self):
        junk_dconf = {"password": "123", "username": "jsmith"}
        junk_session = 123
        credentials, domain = cifutils.getCIFCredentials(junk_dconf,
                                                         junk_session)
        expected_credentials = {"USER": "jsmith", "PASSWD": "123"}
        self.assertEqual(credentials, expected_credentials)
        self.assertEqual(domain, None)

    def test_password_and_username_domain(self):
        junk_dconf = {"cifspassword": "123", "username": "citrix\jsmith"}
        junk_session = 123
        credentials, domain = cifutils.getCIFCredentials(junk_dconf,
                                                         junk_session,
                                                         prefix="cifs")
        expected_credentials = {"USER": "jsmith", "PASSWD": "123"}
        self.assertEqual(credentials, expected_credentials)
        self.assertEqual(domain, "citrix")

    def test_password_and_username_domain_smbsr(self):
        junk_dconf = {"password": "123", "username": "citrix\jsmith"}
        junk_session = 123
        credentials, domain = cifutils.getCIFCredentials(junk_dconf,
                                                         junk_session)
        expected_credentials = {"USER": "jsmith", "PASSWD": "123"}
        self.assertEqual(credentials, expected_credentials)
        self.assertEqual(domain, "citrix")

    @mock.patch('cifutils.util.get_secret', autospec=True)
    def test_password_secret_and_username(self, get_secret):
        junk_dconf = {"cifspassword_secret": "123", "username": "jsmith"}
        junk_session = 123
        get_secret.return_value = 'winter2019'
        credentials, domain = cifutils.getCIFCredentials(junk_dconf,
                                                         junk_session,
                                                         prefix="cifs")
        expected_credentials = {"USER": "jsmith", "PASSWD": "winter2019"}
        get_secret.assert_called_with(junk_session, "123")
        self.assertEqual(credentials, expected_credentials)
        self.assertEqual(domain, None)

    @mock.patch('cifutils.util.get_secret', autospec=True)
    def test_password_secret_and_username_smbsr(self, get_secret):
        junk_dconf = {"password_secret": "123", "username": "jsmith"}
        junk_session = 123
        get_secret.return_value = 'winter2019'
        credentials, domain = cifutils.getCIFCredentials(junk_dconf,
                                                         junk_session)
        expected_credentials = {"USER": "jsmith", "PASSWD": "winter2019"}
        get_secret.assert_called_with(junk_session, "123")
        self.assertEqual(credentials, expected_credentials)
        self.assertEqual(domain, None)

    @mock.patch('cifutils.util.get_secret', autospec=True)
    def test_password_secret_and_username_also_domain(self, get_secret):
        junk_dconf = {"cifspassword_secret": "123",
                      "username": "citrix\jsmith"}
        junk_session = 123
        get_secret.return_value = 'winter2019'
        credentials, domain = cifutils.getCIFCredentials(junk_dconf,
                                                         junk_session,
                                                         prefix="cifs")
        expected_credentials = {"USER": "jsmith", "PASSWD": "winter2019"}
        get_secret.assert_called_with(junk_session, "123")
        self.assertEqual(credentials, expected_credentials)
        self.assertEqual(domain, "citrix")

    @mock.patch('cifutils.util.get_secret', autospec=True)
    def test_password_secret_and_username_also_domain_smbsr(self, get_secret):
        junk_dconf = {"password_secret": "123",
                      "username": "citrix\jsmith"}
        junk_session = 123
        get_secret.return_value = 'winter2019'
        credentials, domain = cifutils.getCIFCredentials(junk_dconf,
                                                         junk_session)
        expected_credentials = {"USER": "jsmith", "PASSWD": "winter2019"}
        get_secret.assert_called_with(junk_session, "123")
        self.assertEqual(credentials, expected_credentials)
        self.assertEqual(domain, "citrix")

    def test_username_bad_domain(self):
        junk_dconf = {"cifspassword_secret": "123",
                      "username": "citrix\gjk\jsmith"}
        junk_session = 123
        with self.assertRaises(cifutils.CIFSException) as cm:
            cifutils.getCIFCredentials(junk_dconf, junk_session, prefix="cifs")
        expected_message = ("A maximum of 2 tokens are expected "
                            "(<domain>\<username>). 3 were given.")
        the_exception = cm.exception
        self.assertEqual(the_exception.errstr, expected_message)

    def test_username_bad_domain_smbsr(self):
        junk_dconf = {"password_secret": "123",
                      "username": "citrix\gjk\jsmith"}
        junk_session = 123
        with self.assertRaises(cifutils.CIFSException) as cm:
            cifutils.getCIFCredentials(junk_dconf, junk_session)
        expected_message = ("A maximum of 2 tokens are expected "
                            "(<domain>\<username>). 3 were given.")
        the_exception = cm.exception
        self.assertEqual(the_exception.errstr, expected_message)

    def test_got_credentials_empty_dconf(self):
        junk_dconf = {}
        self.assertEqual(cifutils.containsCredentials(junk_dconf), False)

    def test_got_credentials_empty_dconf_smbsr(self):
        junk_dconf = {}
        self.assertEqual(cifutils.containsCredentials(junk_dconf), False)

    def test_got_credentials_username_only(self):
        junk_dconf = {'username': 'jsmith'}
        self.assertEqual(cifutils.containsCredentials(junk_dconf), False)

    def test_got_credentials_username_only_smbsr(self):
        junk_dconf = {'username': 'jsmith'}
        self.assertEqual(cifutils.containsCredentials(junk_dconf), False)

    def test_got_credentials_password_only(self):
        junk_dconf = {'cifspassword': 'password123'}
        got_creds = cifutils.containsCredentials(junk_dconf, prefix="cifs")
        self.assertEqual(got_creds, False)

    def test_got_credentials_password_only_smbsr(self):
        junk_dconf = {'password': 'password123'}
        self.assertEqual(cifutils.containsCredentials(junk_dconf), False)

    def test_got_credentials_secret_only(self):
        junk_dconf = {'cifspassword_secret': 'secret123'}
        got_creds = cifutils.containsCredentials(junk_dconf, prefix="cifs")
        self.assertEqual(got_creds, False)

    def test_got_credentials_secret_only_smbsr(self):
        junk_dconf = {'password_secret': 'secret123'}
        self.assertEqual(cifutils.containsCredentials(junk_dconf), False)

    def test_got_credentials_password_and_secret(self):
        junk_dconf = {'cifspassword': 'password123',
                      'cifspassword_secret': 'secret123'}
        got_creds = cifutils.containsCredentials(junk_dconf, prefix="cifs")
        self.assertEqual(got_creds, False)

    def test_got_credentials_password_and_secret_smbsr(self):
        junk_dconf = {'password': 'password123',
                      'password_secret': 'secret123'}
        self.assertEqual(cifutils.containsCredentials(junk_dconf), False)

    def test_got_credentials_user_and_password(self):
        good_dconf = {'username': 'jsmith', 'cifspassword': 'password123'}
        got_cred = cifutils.containsCredentials(good_dconf, prefix="cifs")
        self.assertEqual(got_cred, True)

    def test_got_credentials_user_and_password_smbsr(self):
        good_dconf = {'username': 'jsmith', 'password': 'password123'}
        self.assertEqual(cifutils.containsCredentials(good_dconf), True)

    def test_got_credentials_user_and_secret(self):
        good_dconf = {'username': 'jsmith', 'cifspassword_secret': 'secret123'}
        got_creds = cifutils.containsCredentials(good_dconf, prefix="cifs")
        self.assertEqual(got_creds, True)

    def test_got_credentials_user_and_secret_smbsr(self):
        good_dconf = {'username': 'jsmith', 'password_secret': 'secret123'}
        self.assertEqual(cifutils.containsCredentials(good_dconf), True)

    def test_got_credentials_everything(self):
        good_dconf = {'username': 'jsmith', 'cifspassword': 'password123',
                      'cifspassword_secret': 'secret123'}
        got_creds = cifutils.containsCredentials(good_dconf, prefix="cifs")
        self.assertEqual(got_creds, True)

    def test_got_credentials_everything_but_smbsr(self):
        good_dconf = {'username': 'jsmith', 'cifspassword': 'password123',
                      'cifspassword_secret': 'secret123'}
        self.assertEqual(cifutils.containsCredentials(good_dconf), False)

    def test_got_credentials_everything_smbsr(self):
        good_dconf = {'username': 'jsmith', 'password': 'password123',
                      'password_secret': 'secret123'}
        self.assertEqual(cifutils.containsCredentials(good_dconf), True)

    def test_got_credentials_everything_and_padding(self):
        good_dconf = {'sahara': 'camel', 'username': 'jsmith',
                      'cifspassword': 'password123',
                      'cifspassword_secret': 'secret123',
                      'nile': 'crocodile'}
        got_creds = cifutils.containsCredentials(good_dconf, prefix="cifs")
        self.assertEqual(got_creds, True)

    def test_got_credentials_everything_and_padding_smbsr(self):
        good_dconf = {'sahara': 'camel', 'username': 'jsmith',
                      'password': 'password123',
                      'password_secret': 'secret123',
                      'nile': 'crocodile'}
        self.assertEqual(cifutils.containsCredentials(good_dconf), True)
