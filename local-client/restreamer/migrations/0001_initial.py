import django.db.models.deletion
from django.db import migrations, models
import restreamer.models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='ClientProfile',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('user_id', models.CharField(blank=True, default='', max_length=255, null=True, unique=True)),
            ],
        ),
        migrations.CreateModel(
            name='StreamingEvent',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('identifier', models.CharField(blank=True, default='', max_length=255, null=True, unique=True)),
                ('short_description', models.CharField(blank=True, max_length=20, null=True)),
                ('date_of_event', models.DateTimeField(auto_now_add=True)),
                ('server_ip', models.CharField(blank=True, default='', max_length=250, null=True)),
                ('received_bytes', models.PositiveBigIntegerField(default=0, verbose_name='Received bytes')),
                ('receiving_activated', models.BooleanField(default=False)),
                ('delivering_activated', models.BooleanField(default=False)),
            ],
        ),
        migrations.CreateModel(
            name='ChunkRecord',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('chunk_file', models.FileField(upload_to=restreamer.models.chunk_directory_path)),
                ('data_size', models.IntegerField()),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('md5', models.CharField(blank=True, default='', max_length=50)),
                ('in_process', models.BooleanField(default=False)),
                ('send', models.BooleanField(default=False)),
                ('streaming_event', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='chunks', to='restreamer.streamingevent')),
            ],
            options={
                'indexes': [
                    models.Index(fields=['streaming_event', 'created_at', 'md5'], name='restreamer__streami_a8e4c9_idx'),
                ],
            },
        ),
    ]
