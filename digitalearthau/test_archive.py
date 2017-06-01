from digitalearthau.paths import write_files


def do_an_archive():
    # Create dummy dataset

    test_uuid = 'a3bc7620-dd02-11e6-a5c0-185e0f80a5c0'
    d = write_files({
        'LS7_TEST': {
            'ga-metadata.yaml':
            # There's only one field it cares about in the metadata
                ('id: %s\n' % test_uuid),
            'product': {
                'SOME_DATA.tif': ''
            },
            'package.sha1':
            # Expected sha1sums calculated from command line (eg. sha1sum /tmp/test-dataset/ga-metadata.yaml)
            # Empty file shasum
                'da39a3ee5e6b4b0d3255bfef95601890afd80709\tproduct/SOME_DATA.tif\n'
                # contents of above id line
                'a3bc7620-dd02-11e6-a5c0-185e0f80a5c0\tga-metadata.yaml\n'
        }
    })
    input_path = d.joinpath('ga-metadata.yaml')

    # Mock index
    # Mock MDSS

    # Call archive function
