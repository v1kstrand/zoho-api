import textwrap

from app.parse_mail import parse_mail


def test_parse_mail_with_full_details():
    body = textwrap.dedent(
        """
        What:
        VDS Discovery Project between David Vikstrand and Annette Vikstrand

        Invitee Time Zone:
        Europe/Stockholm

        Who:
        David Vikstrand - Organizer
        info@vdsai.se
        Annette Vikstrand - +46709726438
        vikstrand10@gmail.com

        Where:
        https://us05web.zoom.us/j/89557834332?pwd=token

        Company:
        Pegon AB

        Phone number (Text notifications):
        undefined
        """
    )

    result = parse_mail(body)

    assert result["customer_name"] == "Annette Vikstrand"
    assert result["customer_first_name"] == "Annette"
    assert result["customer_last_name"] == "Vikstrand"
    assert result["customer_email"] == "vikstrand10@gmail.com"
    assert result["customer_phone"] == "+46709726438"
    assert result["company"] == "Pegon AB"
    assert result["sms_opt_phone"] is None


def test_parse_mail_with_missing_who_name_uses_what_line():
    body = textwrap.dedent(
        """
        What:
        VDS Discovery Project between David Vikstrand and David Vikstrand

        Invitee Time Zone:
        Europe/Stockholm

        Who:
        David Vikstrand - Organizer
        info@vdsai.se
        david.vikstrand@gmail.com

        Where:
        https://example.com

        Company:
        vds

        Phone number (Text notifications):
        undefined
        """
    )

    result = parse_mail(body)

    assert result["customer_name"] == "David Vikstrand"
    assert result["customer_first_name"] == "David"
    assert result["customer_last_name"] == "Vikstrand"
    assert result["customer_email"] == "david.vikstrand@gmail.com"
    assert result["customer_phone"] is None
    assert result["company"] == "vds"


def test_parse_mail_with_email_fallback_builds_readable_name():
    body = textwrap.dedent(
        """
        What:
        Discovery call between David Vikstrand and john_doe

        Invitee Time Zone:
        Europe/Stockholm

        Who:
        David Vikstrand - Organizer
        info@vdsai.se
        john.doe@example.com

        Where:
        https://example.com

        Company:
        Example AB

        Phone number (Text notifications):
        undefined
        """
    )

    result = parse_mail(body)

    assert result["customer_name"] == "John Doe"
    assert result["customer_first_name"] == "John"
    assert result["customer_last_name"] == "Doe"
    assert result["customer_email"] == "john.doe@example.com"
